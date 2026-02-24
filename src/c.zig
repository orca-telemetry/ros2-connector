pub const c = @cImport({
    @cInclude("rcl/rcl.h");
    @cInclude("rcl/graph.h");
    @cInclude("rcl/subscription.h");
    @cInclude("rmw/rmw.h");
    @cInclude("rosidl_runtime_c/message_type_support_struct.h");
    @cInclude("rcutils/types/uint8_array.h");
    @cInclude("rosidl_typesupport_introspection_c/message_introspection.h");
    @cInclude("rosidl_runtime_c/message_type_support_struct.h");
    @cInclude("dlfcn.h");
});
