const std = @import("std");

const test_targets = [_]std.Target.Query{
    .{}, // native host
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseSafe });

    // --- ROS dependencies - linked library ------------------------------------
    // gets added in directly to the exe as a linked system library
    const ros_root = b.option([]const u8, "ros_root", "location to the ros Library") orelse "/opt/ros/jazzy";
    const ros_include_path = b.fmt("{s}/include", .{ros_root});
    const ros_lib_path = b.fmt("{s}/lib", .{ros_root});

    // --- Main executable ----------------------------------------------------------
    const exe = b.addExecutable(.{
        .name = "ros_observer",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    exe.root_module.addIncludePath(b.path("vendor/mcap/include"));

    // link ros
    exe.root_module.addIncludePath(.{ .cwd_relative = ros_include_path });
    exe.root_module.addLibraryPath(.{ .cwd_relative = ros_lib_path });

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

    inline for (ros_libs) |pkg| {
        exe.root_module.addIncludePath(.{ .cwd_relative = pkg });
    }
    inline for (ros_pkgs) |pkg| {
        exe.root_module.addIncludePath(.{ .cwd_relative = b.fmt("{s}/include/{s}", .{ ros_root, pkg }) });
    }

    b.installArtifact(exe);

    // --- Check Step (For ZLS Diagnostics) -----------------------------
    const check_step = b.step("check", "Check if the code compiles");

    const exe_check = b.addExecutable(.{
        .name = "check_exe",
        .root_module = exe.root_module,
    });
    check_step.dependOn(&exe_check.step);

    // --- run command --------------------------------------------------------
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    b.step("run", "Run the app").dependOn(&run_cmd.step);

    // --- test step ----------------------------------------------------------
    const test_step = b.step("test", "Run unit tests");
    for (test_targets) |tgt| {
        // we create a specific module for tests to ensure they are built
        // with the 'test' flag enabled and correctly resolved targets.
        const test_mod = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = b.resolveTargetQuery(tgt),
            .optimize = optimize,
            .link_libc = true,
        });

        // re-apply the same dependency logic to the test module
        test_mod.addIncludePath(.{ .cwd_relative = ros_include_path });
        test_mod.addLibraryPath(.{ .cwd_relative = ros_lib_path });

        inline for (ros_pkgs) |pkg| {
            test_mod.addIncludePath(.{ .cwd_relative = b.fmt("{s}/include/{s}", .{ "/op", pkg }) });
        }
        inline for (ros_libs) |lib| {
            test_mod.addIncludePath(.{ .cwd_relative = lib });
        }

        const unit_tests = b.addTest(.{
            .root_module = test_mod,
        });

        const run_unit_tests = b.addRunArtifact(unit_tests);
        test_step.dependOn(&run_unit_tests.step);

        check_step.dependOn(&unit_tests.step);
    }
}
