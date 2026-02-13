#!/usr/bin/env python3

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile
import inspect
import importlib
import sys
from collections import defaultdict


class NetworkDiscoveryNode(Node):
    def __init__(self):
        super().__init__("network_discovery_node")
        self.get_logger().info("Network Discovery Node started")

        # Timer to periodically discover network
        self.timer = self.create_timer(5.0, self.discover_network)

    def discover_network(self):
        """Discover all nodes and topics on the ROS2 network"""
        self.get_logger().info("\n" + "=" * 80)
        self.get_logger().info("ROS2 NETWORK DISCOVERY")
        self.get_logger().info("=" * 80)

        # Get RMW implementation info
        self.print_rmw_info()

        # Discover all nodes
        node_names = self.get_node_names()
        self.get_logger().info(f"\nDiscovered {len(node_names)} nodes:")
        for node_name in node_names:
            self.get_logger().info(f"  - {node_name}")

        # Discover all topics
        self.discover_topics()

    def print_rmw_info(self):
        """Print RMW implementation information"""
        try:
            # Get RMW implementation
            rmw_implementation = self.get_rmw_implementation_identifier()
            self.get_logger().info(f"\nRMW Implementation: {rmw_implementation}")

            # Try to get version info
            try:
                import rclpy

                self.get_logger().info(f"rclpy version: {rclpy.__version__}")
            except AttributeError:
                self.get_logger().info("rclpy version: Not available")

        except Exception as e:
            self.get_logger().error(f"Error getting RMW info: {e}")

    def get_rmw_implementation_identifier(self):
        """Get the RMW implementation identifier"""
        try:
            from rclpy.impl.implementation_singleton import rclpy_implementation

            return rclpy_implementation.rmw_get_implementation_identifier()
        except Exception as e:
            return f"Unknown (Error: {e})"

    def discover_topics(self):
        """Discover all topics and analyze their message types"""
        topic_list = self.get_topic_names_and_types()

        self.get_logger().info(f"\n{'=' * 80}")
        self.get_logger().info(f"Discovered {len(topic_list)} topics:")
        self.get_logger().info(f"{'=' * 80}\n")

        # Group topics by node (best effort)
        for topic_name, type_names in topic_list:
            self.get_logger().info(f"Topic: {topic_name}")

            for type_name in type_names:
                self.get_logger().info(f"  Type: {type_name}")

                # Analyze the message type
                type_info = self.analyze_message_type(type_name)
                self.get_logger().info(f"    Is POD: {type_info['is_pod']}")
                self.get_logger().info(
                    f"    Has nested types: {type_info['has_nested']}"
                )
                self.get_logger().info(f"    Field count: {type_info['field_count']}")

                if type_info["fields"]:
                    self.get_logger().info(f"    Fields:")
                    for field_name, field_type, is_primitive in type_info["fields"]:
                        primitive_marker = (
                            " (primitive)" if is_primitive else " (complex)"
                        )
                        self.get_logger().info(
                            f"      - {field_name}: {field_type}{primitive_marker}"
                        )

            # Get publishers and subscribers for this topic
            pubs = self.get_publishers_info_by_topic(topic_name)
            subs = self.get_subscriptions_info_by_topic(topic_name)

            self.get_logger().info(f"  Publishers: {len(pubs)}")
            for pub in pubs:
                self.get_logger().info(
                    f"    - Node: {pub.node_name}, Namespace: {pub.node_namespace}"
                )

            self.get_logger().info(f"  Subscribers: {len(subs)}")
            for sub in subs:
                self.get_logger().info(
                    f"    - Node: {sub.node_name}, Namespace: {sub.node_namespace}"
                )

            self.get_logger().info("")

    def analyze_message_type(self, type_name):
        """Analyze a ROS2 message type to determine if it's POD and get field info"""
        result = {"is_pod": False, "has_nested": False, "field_count": 0, "fields": []}

        try:
            # Parse the type name (e.g., 'std_msgs/msg/String')
            parts = type_name.split("/")
            if len(parts) != 3:
                return result

            package_name, _, msg_name = parts

            # Import the message module
            module_name = f"{package_name}.msg"
            try:
                msg_module = importlib.import_module(module_name)
            except ImportError:
                self.get_logger().warn(f"Could not import {module_name}")
                return result

            # Get the message class
            if not hasattr(msg_module, msg_name):
                return result

            msg_class = getattr(msg_module, msg_name)

            # Try multiple methods to get field information
            fields_and_types = None

            # Method 1: Try get_fields_and_field_types() class method
            if hasattr(msg_class, "get_fields_and_field_types"):
                try:
                    fields_and_types = msg_class.get_fields_and_field_types()
                except:
                    pass

            # Method 2: Try FIELDS_AND_FIELD_TYPES attribute
            if fields_and_types is None and hasattr(
                msg_class, "FIELDS_AND_FIELD_TYPES"
            ):
                fields_and_types = msg_class.FIELDS_AND_FIELD_TYPES

            # Method 3: Try __slots__ (for newer ROS2 versions)
            if fields_and_types is None and hasattr(msg_class, "__slots__"):
                # Create instance to inspect fields
                try:
                    instance = msg_class()
                    fields_and_types = {}
                    for slot in msg_class.__slots__:
                        if slot.startswith("_"):
                            continue
                        attr_value = getattr(instance, slot, None)
                        if attr_value is not None:
                            fields_and_types[slot] = type(attr_value).__name__
                        else:
                            # Try to get type from annotations
                            if hasattr(msg_class, "__annotations__"):
                                ann_type = msg_class.__annotations__.get(
                                    slot, "unknown"
                                )
                                fields_and_types[slot] = str(ann_type)
                            else:
                                fields_and_types[slot] = "unknown"
                except:
                    pass

            if fields_and_types:
                result["field_count"] = len(fields_and_types)

                # Check each field
                all_primitive = True
                for field_name, field_type in fields_and_types.items():
                    is_primitive = self.is_primitive_type(str(field_type))
                    result["fields"].append((field_name, str(field_type), is_primitive))

                    if not is_primitive:
                        all_primitive = False
                        result["has_nested"] = True

                # Message is POD if all fields are primitive types
                result["is_pod"] = all_primitive and result["field_count"] > 0

        except Exception as e:
            self.get_logger().warn(f"Error analyzing type {type_name}: {e}")

        return result

    def is_primitive_type(self, type_str):
        """Check if a type string represents a primitive type"""
        # ROS2 primitive types
        primitives = {
            "boolean",
            "bool",
            "byte",
            "char",
            "uint8",
            "int8",
            "uint16",
            "int16",
            "uint32",
            "int32",
            "uint64",
            "int64",
            "float32",
            "float64",
            "string",
            "wstring",
        }

        # Remove array brackets if present
        base_type = type_str.split("[")[0].strip()

        # Check if it's a primitive
        return base_type in primitives


def main(args=None):
    rclpy.init(args=args)

    node = NetworkDiscoveryNode()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
