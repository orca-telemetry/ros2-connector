const std = @import("std");

pub fn build(b: *std.Build) void {
    const exe = b.addExecutable(.{
        .name = "ros2_discovery_zig",
    });

    // Add ROS2 include paths
    // Adjust this path based on your ROS2 distribution
    const ros_distro = std.process.getEnvVarOwned(b.allocator, "ROS_DISTRO") catch "jazzy";
    defer b.allocator.free(ros_distro);

    const ros_path = std.fmt.allocPrint(
        b.allocator,
        "/opt/ros/{s}/include",
        .{ros_distro},
    ) catch unreachable;
    defer b.allocator.free(ros_path);

    // Add include directories
    exe.addIncludePath(.{ .cwd_relative = ros_path });
    exe.addIncludePath(.{ .cwd_relative = b.fmt("/opt/ros/{s}/include/rcl", .{ros_distro}) });
    exe.addIncludePath(.{ .cwd_relative = b.fmt("/opt/ros/{s}/include/rcutils", .{ros_distro}) });
    exe.addIncludePath(.{ .cwd_relative = b.fmt("/opt/ros/{s}/include/rmw", .{ros_distro}) });
    exe.addIncludePath(.{ .cwd_relative = b.fmt("/opt/ros/{s}/include/rosidl_runtime_c", .{ros_distro}) });
    exe.addIncludePath(.{ .cwd_relative = b.fmt("/opt/ros/{s}/include/rosidl_typesupport_interface", .{ros_distro}) });
    exe.addIncludePath(.{ .cwd_relative = b.fmt("/opt/ros/{s}/include/rcl_interfaces", .{ros_distro}) });

    // Add library paths
    exe.addLibraryPath(.{ .cwd_relative = b.fmt("/opt/ros/{s}/lib", .{ros_distro}) });

    // Link ROS2 libraries
    exe.linkSystemLibrary("rcl");
    exe.linkSystemLibrary("rcutils");
    exe.linkSystemLibrary("rmw");
    exe.linkSystemLibrary("rosidl_runtime_c");
    exe.linkSystemLibrary("rcl_interfaces__rosidl_typesupport_c");

    exe.linkLibC();

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);
}
