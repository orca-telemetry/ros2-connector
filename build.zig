const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseFast });

    // --- nanoarrow (core + IPC) -------------------------------------------------
    const nanoarrow_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    const nanoarrow = b.addLibrary(.{
        .name = "nanoarrow",
        .root_module = nanoarrow_mod,
        .linkage = .static,
    });
    nanoarrow.addCSourceFiles(.{
        .files = &.{
            "vendor/nanoarrow/nanoarrow.c",
            "vendor/nanoarrow/flatcc.c",
            "vendor/nanoarrow/nanoarrow_ipc.c",
            "vendor/nanoarrow/arrow_helpers.c",
        },
        .flags = &.{"-std=c11"},
    });

    nanoarrow.addIncludePath(b.path("vendor/nanoarrow"));
    nanoarrow_mod.link_libc = true;

    // --- main executable --------------------------------------------------------
    const exe = b.addExecutable(.{
        .name = "ros_observer",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    exe.root_module.strip = true;
    exe.link_gc_sections = true;
    exe.root_module.link_libc = true;

    // --- ROS 2 includes ---------------------------------------------------------
    // Override at build time with -Dros-root=/opt/ros/jazzy etc.
    const ros_root = b.option(
        []const u8,
        "ros-root",
        "ROS 2 install root (default: /opt/ros/jazzy)",
    ) orelse "/opt/ros/jazzy";

    // All ROS 2 packages follow the same include/<pkg>/<pkg>/ convention.
    // Add the top-level include dir plus each package subdirectory.
    const ros_include = b.fmt("{s}/include", .{ros_root});
    exe.root_module.addIncludePath(.{ .cwd_relative = ros_include });

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
        exe.root_module.addIncludePath(.{
            .cwd_relative = b.fmt("{s}/include/{s}", .{ ros_root, pkg }),
        });
    }

    // --- ROS 2 libs -------------------------------------------------------------
    const ros_lib = b.fmt("{s}/lib", .{ros_root});
    exe.root_module.addLibraryPath(.{ .cwd_relative = ros_lib });
    exe.root_module.addRPath(.{ .cwd_relative = ros_lib });

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
        exe.root_module.linkSystemLibrary(lib, .{});
    }

    // --- nanoarrow --------------------------------------------------------------
    exe.linkLibrary(nanoarrow);
    exe.root_module.addIncludePath(b.path("vendor/nanoarrow"));

    b.installArtifact(exe);

    // --- run --------------------------------------------------------------------
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    b.step("run", "Run the app").dependOn(&run_cmd.step);

    // --- test -------------------------------------------------------------------
    const tests = b.addTest(.{ .root_module = exe.root_module });
    b.step("test", "Run tests").dependOn(&b.addRunArtifact(tests).step);
}
