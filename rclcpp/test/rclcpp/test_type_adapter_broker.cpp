// Copyright 2026 nevlife
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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rcl/intra_process.h"
#include "rclcpp/rclcpp.hpp"
#include "test_msgs/msg/strings.hpp"

using namespace std::chrono_literals;

struct Text
{
  Text() = default;
  explicit Text(std::string value)
  : data(std::move(value)) {}
  Text(const Text & other)
  : data(other.data) {++copies;}
  std::string data;
  static std::atomic<int> copies;
};
std::atomic<int> Text::copies{0};

struct SharedText
{
  std::shared_ptr<const test_msgs::msg::Strings> message;
};

std::atomic<int> to_ros{0};
std::atomic<int> to_custom{0};
std::atomic<int> to_shared{0};

template<>
struct rclcpp::TypeAdapter<Text, test_msgs::msg::Strings>
{
  using is_specialized = std::true_type;
  using custom_type = Text;
  using ros_message_type = test_msgs::msg::Strings;

  static void convert_to_ros_message(const custom_type & source, ros_message_type & destination)
  {
    ++to_ros;
    destination.string_value = source.data;
  }

  static void convert_to_custom(const ros_message_type & source, custom_type & destination)
  {
    ++to_custom;
    destination.data = source.string_value;
  }
};

template<>
struct rclcpp::TypeAdapter<SharedText, test_msgs::msg::Strings>
{
  using is_specialized = std::true_type;
  using custom_type = SharedText;
  using ros_message_type = test_msgs::msg::Strings;

  static void convert_to_ros_message(const custom_type & source, ros_message_type & destination)
  {
    destination = *source.message;
  }

  static void convert_to_custom(const ros_message_type & source, custom_type & destination)
  {
    ++to_custom;
    destination.message = std::make_shared<ros_message_type>(source);
  }

  static std::shared_ptr<const custom_type> convert_to_custom(
    std::shared_ptr<const ros_message_type> source)
  {
    ++to_shared;
    return std::make_shared<custom_type>(custom_type{std::move(source)});
  }
};

using TextAdapter = rclcpp::TypeAdapter<Text, test_msgs::msg::Strings>;
using SharedTextAdapter = rclcpp::TypeAdapter<SharedText, test_msgs::msg::Strings>;

class TestTypeAdapterBroker : public ::testing::Test
{
protected:
  void SetUp() override
  {
    setenv("RCL_INTRA_PROCESS", "1", 1);
    rclcpp::init(0, nullptr);
    node = std::make_shared<rclcpp::Node>("type_adapter_broker");
    executor = std::make_unique<rclcpp::executors::SingleThreadedExecutor>();
    executor->add_node(node);
    to_ros = 0;
    to_custom = 0;
    to_shared = 0;
    Text::copies = 0;
  }

  void TearDown() override
  {
    executor.reset();
    node.reset();
    rclcpp::shutdown();
  }

  template<typename PublisherT>
  void require_broker(const PublisherT & publisher, size_t subscriptions)
  {
    if (!rcl_publisher_is_ipc_enabled(publisher->get_publisher_handle().get())) {
      GTEST_SKIP() << "the rmw has no intra-process routing";
    }
    for (int i = 0; i < 200 && publisher->get_subscription_count() < subscriptions; ++i) {
      std::this_thread::sleep_for(5ms);
    }
    ASSERT_EQ(subscriptions, publisher->get_subscription_count());
  }

  void spin_until(const std::function<bool()> & done)
  {
    for (int i = 0; i < 400 && !done(); ++i) {
      executor->spin_some(5ms);
    }
    ASSERT_TRUE(done());
  }

  rclcpp::Node::SharedPtr node;
  std::unique_ptr<rclcpp::executors::SingleThreadedExecutor> executor;
};

TEST_F(TestTypeAdapterBroker, adapted_reader_gets_the_published_object)
{
  std::vector<std::shared_ptr<const Text>> received;
  auto sub = node->create_subscription<TextAdapter>(
    "text", 5, [&](std::shared_ptr<const Text> text) {received.push_back(text);});
  auto pub = node->create_publisher<TextAdapter>("text", 5);
  require_broker(pub, 1);
  if (HasFatalFailure() || IsSkipped()) {return;}
  auto text = std::make_unique<Text>("hello");
  const Text * address = text.get();
  pub->publish(std::move(text));
  spin_until([&] {return received.size() == 1;});
  EXPECT_EQ(address, received[0].get());
  EXPECT_EQ("hello", received[0]->data);
  EXPECT_EQ(0, to_ros.load());
  EXPECT_EQ(0, to_custom.load());
  EXPECT_EQ(0, Text::copies.load());
}

TEST_F(TestTypeAdapterBroker, ros_readers_share_one_conversion)
{
  std::vector<std::string> received;
  auto first = node->create_subscription<test_msgs::msg::Strings>(
    "text", 5, [&](const test_msgs::msg::Strings & msg) {received.push_back(msg.string_value);});
  auto second = node->create_subscription<test_msgs::msg::Strings>(
    "text", 5, [&](const test_msgs::msg::Strings & msg) {received.push_back(msg.string_value);});
  std::vector<std::shared_ptr<const Text>> adapted;
  auto third = node->create_subscription<TextAdapter>(
    "text", 5, [&](std::shared_ptr<const Text> text) {adapted.push_back(text);});
  auto pub = node->create_publisher<TextAdapter>("text", 5);
  require_broker(pub, 3);
  if (HasFatalFailure() || IsSkipped()) {return;}
  pub->publish(Text("const reference"));
  spin_until([&] {return received.size() == 2 && adapted.size() == 1;});
  EXPECT_EQ("const reference", received[0]);
  EXPECT_EQ("const reference", received[1]);
  EXPECT_EQ("const reference", adapted[0]->data);
  EXPECT_EQ(1, to_ros.load());
  EXPECT_EQ(0, to_custom.load());
  EXPECT_EQ(1, Text::copies.load());
}

TEST_F(TestTypeAdapterBroker, unique_reader_gets_its_own_copy)
{
  std::unique_ptr<Text> received;
  auto sub = node->create_subscription<TextAdapter>(
    "text", 5, [&](std::unique_ptr<Text> text) {received = std::move(text);});
  auto pub = node->create_publisher<TextAdapter>("text", 5);
  require_broker(pub, 1);
  if (HasFatalFailure() || IsSkipped()) {return;}
  auto text = std::make_unique<Text>("mutable");
  const Text * address = text.get();
  pub->publish(std::move(text));
  spin_until([&] {return received != nullptr;});
  EXPECT_NE(address, received.get());
  EXPECT_EQ("mutable", received->data);
  EXPECT_EQ(1, Text::copies.load());
  EXPECT_EQ(0, to_ros.load());
}

TEST_F(TestTypeAdapterBroker, ros_message_reaches_const_adapted_reader_without_copy)
{
  std::vector<std::shared_ptr<const SharedText>> received;
  auto sub = node->create_subscription<SharedTextAdapter>(
    "text", 5, [&](std::shared_ptr<const SharedText> text) {received.push_back(text);});
  std::vector<std::shared_ptr<const SharedText>> by_reference;
  auto ref_sub = node->create_subscription<SharedTextAdapter>(
    "text", 5, [&](const SharedText & text) {
      by_reference.push_back(std::make_shared<SharedText>(text));
    });
  auto pub = node->create_publisher<test_msgs::msg::Strings>("text", 5);
  require_broker(pub, 2);
  if (HasFatalFailure() || IsSkipped()) {return;}
  auto msg = std::make_unique<test_msgs::msg::Strings>();
  msg->string_value = "shared";
  const test_msgs::msg::Strings * address = msg.get();
  pub->publish(std::move(msg));
  spin_until([&] {return received.size() == 1 && by_reference.size() == 1;});
  EXPECT_EQ(address, received[0]->message.get());
  EXPECT_EQ(address, by_reference[0]->message.get());
  EXPECT_EQ(2, to_shared.load());
  EXPECT_EQ(0, to_custom.load());
}

TEST_F(TestTypeAdapterBroker, mutable_adapted_reader_still_converts)
{
  std::unique_ptr<SharedText> received;
  auto sub = node->create_subscription<SharedTextAdapter>(
    "text", 5, [&](std::unique_ptr<SharedText> text) {received = std::move(text);});
  auto pub = node->create_publisher<test_msgs::msg::Strings>("text", 5);
  require_broker(pub, 1);
  if (HasFatalFailure() || IsSkipped()) {return;}
  test_msgs::msg::Strings msg;
  msg.string_value = "copied";
  pub->publish(msg);
  spin_until([&] {return received != nullptr;});
  EXPECT_EQ("copied", received->message->string_value);
  EXPECT_EQ(0, to_shared.load());
  EXPECT_EQ(1, to_custom.load());
}

TEST_F(TestTypeAdapterBroker, only_mutable_ros_readers_copy_a_broker_message)
{
  std::vector<const test_msgs::msg::Strings *> seen;
  auto mutable_sub = node->create_subscription<test_msgs::msg::Strings>(
    "text", 5, [&](std::shared_ptr<test_msgs::msg::Strings> msg) {
      seen.push_back(msg.get());
      msg->string_value = "changed";
    });
  auto ref_sub = node->create_subscription<test_msgs::msg::Strings>(
    "text", 5, [&](const test_msgs::msg::Strings & msg) {
      seen.push_back(&msg);
      EXPECT_EQ("original", msg.string_value);
    });
  auto pub = node->create_publisher<test_msgs::msg::Strings>("text", 5);
  require_broker(pub, 2);
  if (HasFatalFailure() || IsSkipped()) {return;}
  auto msg = std::make_unique<test_msgs::msg::Strings>();
  msg->string_value = "original";
  const test_msgs::msg::Strings * address = msg.get();
  pub->publish(std::move(msg));
  spin_until([&] {return seen.size() == 2;});
  EXPECT_EQ(1, std::count(seen.begin(), seen.end(), address));
}
