const std = @import("std");

const test_targets = [_]std.Target.Query{
    .{}, // native host
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseSmall });

    // --- ROS dependencies - linked library ------------------------------------
    const ros_root = b.option([]const u8, "ros_root", "location to the ros Library") orelse "/opt/ros/jazzy";
    const ros_include_path = b.fmt("{s}/include", .{ros_root});
    const ros_lib_path = b.fmt("{s}/lib", .{ros_root});

    // --- MCAP C++ bridge (compiled with system g++ to match libstdc++ ABI) ----
    const compile_bridge = b.addSystemCommand(&.{
        "g++", "-shared",                                     "-fPIC", "-O2",
        "-I",  b.fmt("{s}/include/mcap_vendor", .{ros_root}), "-L",    ros_lib_path,
        "-o",
    });
    const bridge_so = compile_bridge.addOutputFileArg("libmcap_bridge.so");
    compile_bridge.addFileArg(b.path("src/mcap_bridge.cpp"));
    compile_bridge.addArg("-lmcap");
    compile_bridge.addArg(b.fmt("-Wl,-rpath,{s}", .{ros_lib_path}));

    // Install bridge .so alongside the binary
    b.getInstallStep().dependOn(&b.addInstallFileWithDir(
        bridge_so,
        .bin,
        "libmcap_bridge.so",
    ).step);

    // --- Main executable ----------------------------------------------------------
    const exe = b.addExecutable(.{
        .name = "orca",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });

    // ROS include paths
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
    };

    inline for (ros_pkgs) |pkg| {
        exe.root_module.addIncludePath(.{ .cwd_relative = b.fmt("{s}/include/{s}", .{ ros_root, pkg }) });
    }
    inline for (ros_libs) |lib| {
        exe.root_module.linkSystemLibrary(lib, .{});
    }

    // Link MCAP bridge shared library; rpath=$ORIGIN so it finds libmcap_bridge.so next to binary
    exe.root_module.addObjectFile(bridge_so);
    exe.root_module.addRPath(.{ .cwd_relative = "$ORIGIN" });

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
            test_mod.addIncludePath(.{ .cwd_relative = b.fmt("{s}/include/{s}", .{ ros_root, pkg }) });
        }
        inline for (ros_libs) |lib| {
            test_mod.linkSystemLibrary(lib, .{});
        }

        // Link MCAP bridge shared library
        test_mod.addObjectFile(bridge_so);
        test_mod.addRPath(.{ .cwd_relative = "$ORIGIN" });

        const unit_tests = b.addTest(.{
            .root_module = test_mod,
        });

        const run_unit_tests = b.addRunArtifact(unit_tests);
        test_step.dependOn(&run_unit_tests.step);

        check_step.dependOn(&unit_tests.step);
    }
}
