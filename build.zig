const std = @import("std");

const test_targets = [_]std.Target.Query{
    .{}, // native host
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseSafe });

    // --- 1. Define Dependencies (nanoarrow) ------------------------------------
    const nanoarrow_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    const nanoarrow_lib = b.addLibrary(.{
        .name = "nanoarrow",
        .root_module = nanoarrow_mod,
        .linkage = .static,
    });

    nanoarrow_lib.addCSourceFiles(.{
        .files = &.{
            "vendor/nanoarrow/nanoarrow.c",
            "vendor/nanoarrow/flatcc.c",
            "vendor/nanoarrow/nanoarrow_ipc.c",
            "vendor/nanoarrow/arrow_helpers.c",
        },
        .flags = &.{"-std=c11"},
    });
    nanoarrow_lib.addIncludePath(b.path("vendor/nanoarrow"));

    // --- 2. Configure the Main Module ------------------------------------------
    const main_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    // Link nanoarrow to the main module
    main_mod.addIncludePath(b.path("vendor/nanoarrow"));

    // --- ROS 2 Configuration ---
    const ros_root = b.option(
        []const u8,
        "ros-root",
        "ROS 2 install root (default: /opt/ros/jazzy)",
    ) orelse "/opt/ros/jazzy";

    const ros_include = b.fmt("{s}/include", .{ros_root});
    const ros_lib_path = b.fmt("{s}/lib", .{ros_root});

    main_mod.addIncludePath(.{ .cwd_relative = ros_include });
    main_mod.addLibraryPath(.{ .cwd_relative = ros_lib_path });

    const ros_pkgs = [_][]const u8{
        "rcl",
        "rcutils",
        "rmw",
        "rcl_yaml_param_parser",
        "type_description_interfaces",
        "rosidl_runtime_c",
        "service_msgs",
        "builtin_interfaces",
        "rosidl_typesupport_interface",
        "rosidl_dynamic_typesupport",
        "rosidl_typesupport_introspection_c",
    };
    inline for (ros_pkgs) |pkg| {
        main_mod.addIncludePath(.{ .cwd_relative = pkg });
    }

    const ros_libs = [_][]const u8{
        "rcl",
        "rcutils",
        "rmw",
        "rmw_implementation",
        "rcl_yaml_param_parser",
        "rosidl_runtime_c",
        "rosidl_typesupport_c",
        "rosidl_typesupport_introspection_c",
        "dl",
    };
    inline for (ros_libs) |lib| {
        main_mod.linkSystemLibrary(lib, .{});
    }

    // --- 3. Main Executable ----------------------------------------------------
    const exe = b.addExecutable(.{
        .name = "ros_observer",
        .root_module = main_mod,
    });
    exe.linkLibrary(nanoarrow_lib);
    b.installArtifact(exe);

    // --- 4. The "Check" Step (For ZLS Diagnostics) -----------------------------
    const check_step = b.step("check", "Check if the code compiles");

    const exe_check = b.addExecutable(.{
        .name = "check_exe",
        .root_module = main_mod,
    });
    exe_check.linkLibrary(nanoarrow_lib);
    check_step.dependOn(&exe_check.step);

    // --- 5. Run Command --------------------------------------------------------
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    b.step("run", "Run the app").dependOn(&run_cmd.step);

    // --- 6. Test Step ----------------------------------------------------------
    const test_step = b.step("test", "Run unit tests");
    for (test_targets) |tgt| {
        // We create a specific module for tests to ensure they are built
        // with the 'test' flag enabled and correctly resolved targets.
        const test_mod = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = b.resolveTargetQuery(tgt),
            .optimize = optimize,
            .link_libc = true,
        });

        // Re-apply the same dependency logic to the test module
        test_mod.addIncludePath(b.path("vendor/nanoarrow"));
        test_mod.addIncludePath(.{ .cwd_relative = ros_include });
        test_mod.addLibraryPath(.{ .cwd_relative = ros_lib_path });
        inline for (ros_pkgs) |pkg| {
            test_mod.addIncludePath(.{ .cwd_relative = b.fmt("{s}/include/{s}", .{ ros_root, pkg }) });
        }
        inline for (ros_libs) |lib| {
            test_mod.linkSystemLibrary(lib, .{});
        }

        const unit_tests = b.addTest(.{
            .root_module = test_mod,
        });
        unit_tests.linkLibrary(nanoarrow_lib);

        const run_unit_tests = b.addRunArtifact(unit_tests);
        test_step.dependOn(&run_unit_tests.step);

        check_step.dependOn(&unit_tests.step);
    }
}
