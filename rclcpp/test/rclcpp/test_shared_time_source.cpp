// Copyright 2026 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rosgraph_msgs/msg/clock.hpp"

#include "rclcpp/rclcpp.hpp"

using namespace std::chrono_literals;

class TestSharedTimeSource : public ::testing::Test
{
protected:
  void SetUp() override
  {
    rclcpp::init(0, nullptr);
    observer_ = std::make_shared<rclcpp::Node>("shared_time_source_observer");
    clock_pub_ = observer_->create_publisher<rosgraph_msgs::msg::Clock>(
      "/clock", rclcpp::ClockQoS());
  }

  void TearDown() override
  {
    clock_pub_.reset();
    observer_.reset();
    rclcpp::shutdown();
  }

  // The time source handles changes of use_sim_time from /parameter_events, spin_node spins it.
  static bool wait_for(
    const std::function<bool()> & condition, rclcpp::Node::SharedPtr spin_node = nullptr)
  {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!condition()) {
      if (std::chrono::steady_clock::now() > deadline) {
        return false;
      }
      if (spin_node) {
        rclcpp::spin_some(spin_node);
      }
      std::this_thread::sleep_for(10ms);
    }
    return true;
  }

  static bool set_use_sim_time(rclcpp::Node::SharedPtr node, bool value)
  {
    if (!node->set_parameter(rclcpp::Parameter("use_sim_time", value)).successful) {
      return false;
    }
    return wait_for(
      [&]() {return node->get_clock()->ros_time_is_active() == value;}, node);
  }

  size_t clock_subscriptions() const {return observer_->count_subscribers("/clock");}

  bool wait_for_clock_subscriptions(size_t expected) const
  {
    return wait_for([this, expected]() {return clock_subscriptions() == expected;});
  }

  // Publishes until every clock reads the time, as the subscriptions may still be matching.
  bool publish_until_seen(
    int32_t sec, const std::vector<rclcpp::Clock::SharedPtr> & clocks) const
  {
    rosgraph_msgs::msg::Clock msg;
    msg.clock.sec = sec;
    return wait_for(
      [&]() {
        clock_pub_->publish(msg);
        for (const auto & clock : clocks) {
          if (clock->now().seconds() != sec) {
            return false;
          }
        }
        return true;
      });
  }

  static rclcpp::NodeOptions sim_time_options()
  {
    return rclcpp::NodeOptions().append_parameter_override("use_sim_time", true);
  }

  rclcpp::Node::SharedPtr observer_;
  rclcpp::Publisher<rosgraph_msgs::msg::Clock>::SharedPtr clock_pub_;
};

TEST_F(TestSharedTimeSource, nodes_share_one_clock_subscription)
{
  auto source = std::make_shared<rclcpp::Node>("source", sim_time_options());
  auto options = rclcpp::NodeOptions().time_source(source->get_node_time_source_interface());
  std::vector<rclcpp::Node::SharedPtr> sharing;
  for (int i = 0; i < 4; ++i) {
    sharing.push_back(std::make_shared<rclcpp::Node>("sharing_" + std::to_string(i), options));
  }
  ASSERT_TRUE(wait_for_clock_subscriptions(1u));

  std::vector<rclcpp::Clock::SharedPtr> clocks{source->get_clock()};
  for (const auto & node : sharing) {
    EXPECT_EQ(node->get_node_time_source_interface(), source->get_node_time_source_interface());
    EXPECT_FALSE(node->has_parameter("use_sim_time"));
    EXPECT_TRUE(node->get_clock()->ros_time_is_active());
    clocks.push_back(node->get_clock());
  }
  EXPECT_TRUE(publish_until_seen(42, clocks));

  // Without sharing every node subscribes to /clock.
  std::vector<rclcpp::Node::SharedPtr> separate;
  for (int i = 0; i < 4; ++i) {
    separate.push_back(
      std::make_shared<rclcpp::Node>("separate_" + std::to_string(i), sim_time_options()));
  }
  EXPECT_TRUE(wait_for_clock_subscriptions(5u));
}

TEST_F(TestSharedTimeSource, use_sim_time_of_the_source_switches_all_clocks)
{
  auto source = std::make_shared<rclcpp::Node>("source");
  auto sharing = std::make_shared<rclcpp::Node>(
    "sharing", rclcpp::NodeOptions().time_source(source->get_node_time_source_interface()));
  auto another = std::make_shared<rclcpp::Node>(
    "sharing_too", rclcpp::NodeOptions().time_source(source->get_node_time_source_interface()));
  EXPECT_FALSE(sharing->get_clock()->ros_time_is_active());
  EXPECT_EQ(clock_subscriptions(), 0u);

  ASSERT_TRUE(set_use_sim_time(source, true));
  EXPECT_TRUE(sharing->get_clock()->ros_time_is_active());
  EXPECT_TRUE(another->get_clock()->ros_time_is_active());
  ASSERT_TRUE(wait_for_clock_subscriptions(1u));
  EXPECT_TRUE(publish_until_seen(7, {sharing->get_clock(), another->get_clock()}));

  ASSERT_TRUE(set_use_sim_time(source, false));
  EXPECT_FALSE(sharing->get_clock()->ros_time_is_active());
  EXPECT_FALSE(another->get_clock()->ros_time_is_active());
  EXPECT_TRUE(wait_for_clock_subscriptions(0u));

  ASSERT_TRUE(set_use_sim_time(source, true));
  EXPECT_TRUE(sharing->get_clock()->ros_time_is_active());
  EXPECT_TRUE(wait_for_clock_subscriptions(1u));
}

TEST_F(TestSharedTimeSource, sharing_node_destroyed_first)
{
  auto source = std::make_shared<rclcpp::Node>("source", sim_time_options());
  auto sharing = std::make_shared<rclcpp::Node>(
    "sharing", rclcpp::NodeOptions().time_source(source->get_node_time_source_interface()));
  auto sub_node = sharing->create_sub_node("sub");
  auto sharing_clock = sharing->get_clock();
  ASSERT_TRUE(publish_until_seen(1, {source->get_clock(), sharing_clock}));

  // A sub-node shares the clock of its parent and must not detach it.
  sub_node.reset();
  ASSERT_TRUE(publish_until_seen(2, {source->get_clock(), sharing_clock}));

  sharing.reset();
  ASSERT_TRUE(publish_until_seen(3, {source->get_clock()}));
  EXPECT_EQ(sharing_clock->now().seconds(), 2);
  EXPECT_EQ(clock_subscriptions(), 1u);

  source.reset();
  EXPECT_TRUE(wait_for_clock_subscriptions(0u));
}

TEST_F(TestSharedTimeSource, source_node_destroyed_first)
{
  auto source = std::make_shared<rclcpp::Node>("source", sim_time_options());
  auto sharing = std::make_shared<rclcpp::Node>(
    "sharing", rclcpp::NodeOptions().time_source(source->get_node_time_source_interface()));
  auto source_clock = source->get_clock();
  ASSERT_TRUE(publish_until_seen(1, {sharing->get_clock()}));

  source.reset();
  ASSERT_TRUE(publish_until_seen(2, {sharing->get_clock()}));
  EXPECT_EQ(source_clock->now().seconds(), 1);
  EXPECT_EQ(clock_subscriptions(), 1u);

  sharing.reset();
  EXPECT_TRUE(wait_for_clock_subscriptions(0u));
}

TEST_F(TestSharedTimeSource, options_keep_a_weak_reference)
{
  auto source = std::make_shared<rclcpp::Node>("source");
  auto options = rclcpp::NodeOptions().time_source(source->get_node_time_source_interface());
  auto copy = options;
  rclcpp::NodeOptions assigned;
  assigned = options;
  EXPECT_EQ(copy.time_source(), source->get_node_time_source_interface());
  EXPECT_EQ(assigned.time_source(), source->get_node_time_source_interface());

  source.reset();
  EXPECT_EQ(options.time_source(), nullptr);
  auto own = std::make_shared<rclcpp::Node>("own", options);
  EXPECT_TRUE(own->has_parameter("use_sim_time"));

  options.time_source(own->get_node_time_source_interface());
  options.time_source(nullptr);
  EXPECT_EQ(options.time_source(), nullptr);
  auto other_own = std::make_shared<rclcpp::Node>("other_own", options);
  EXPECT_NE(other_own->get_node_time_source_interface(), own->get_node_time_source_interface());
}

class NotATimeSource : public rclcpp::node_interfaces::NodeTimeSourceInterface
{
};

TEST_F(TestSharedTimeSource, only_node_time_source_is_accepted)
{
  rclcpp::NodeOptions options;
  EXPECT_THROW(
    options.time_source(std::make_shared<NotATimeSource>()), std::invalid_argument);
  EXPECT_EQ(options.time_source(), nullptr);
}

TEST_F(TestSharedTimeSource, non_ros_clock_is_rejected_while_sim_time_is_active)
{
  auto source = std::make_shared<rclcpp::Node>("source", sim_time_options());
  auto options = rclcpp::NodeOptions()
    .time_source(source->get_node_time_source_interface())
    .clock_type(RCL_SYSTEM_TIME);
  EXPECT_THROW(std::make_shared<rclcpp::Node>("sharing", options), std::invalid_argument);
}
