// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include <pybind11/pybind11.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include "rcl/intra_process.h"
#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/image.hpp"

namespace py = pybind11;
using Image = sensor_msgs::msg::Image;
int64_t now()
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
}
class Container
{
public:
  Container(bool cpp_pub, bool cpp_sub, bool legacy, const std::string & topic)
  {
    context_ = std::make_shared<rclcpp::Context>();
    context_->init(0, nullptr);
    auto options = rclcpp::NodeOptions().context(context_).use_intra_process_comms(legacy)
      .start_parameter_services(false).start_parameter_event_publisher(false).enable_rosout(false);
    node_ = std::make_shared<rclcpp::Node>("cpp_component", options);
    if (cpp_pub) {pub_ = node_->create_publisher<Image>(topic, 32);}
    if (cpp_sub) {
      sub_ = node_->create_subscription<Image>(topic, 32, [this](Image::ConstSharedPtr message) {
            auto elapsed = now() - (int64_t(message->header.stamp.sec) * 1000000000 +
            message->header.stamp.nanosec);
            bool valid = std::all_of(message->data.begin(), message->data.end(),
            [](uint8_t value) {return value == 42;});
            std::lock_guard<std::mutex> lock(mutex_);
            valid_ = valid_ && valid;
            sequence_ = message->height;
            latency_ = elapsed;
            ++count_;
            condition_.notify_all();
        });
    }
    rclcpp::ExecutorOptions eo;
    eo.context = context_;
    executor_ = std::make_shared<rclcpp::executors::SingleThreadedExecutor>(eo);
    executor_->add_node(node_);
    thread_ = std::thread([this] {executor_->spin();});
  }
  ~Container() {close();}
  py::capsule lease()
  {
    auto lease = new std::shared_ptr<rcl_context_t>(context_->get_rcl_context());
    return py::capsule(lease, "rcl_context_shared_ptr", [](PyObject * object) {
               delete static_cast<std::shared_ptr<rcl_context_t> *>(
                 PyCapsule_GetPointer(object, "rcl_context_shared_ptr"));
      });
  }
  int64_t publish(size_t size, uint32_t sequence)
  {
    auto message = std::make_unique<Image>();
    message->data.resize(size);
    std::memset(message->data.data(), 42, size);
    message->height = sequence;
    message->width = static_cast<uint32_t>(size);
    message->step = message->width;
    message->header.frame_id = "cpp";
    auto start = now();
    message->header.stamp.sec = static_cast<int32_t>(start / 1000000000);
    message->header.stamp.nanosec = static_cast<uint32_t>(start % 1000000000);
    pub_->publish(std::move(message));
    return start;
  }
  int64_t wait(uint32_t sequence)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!condition_.wait_for(lock, std::chrono::seconds(3), [&] {
        return sequence_ == sequence;
    })) {throw std::runtime_error("C++ callback timeout");}
    if (!valid_) {throw std::runtime_error("payload differs");}
    return latency_;
  }
  size_t count() {std::lock_guard<std::mutex> lock(mutex_); return count_;}
  bool enabled()
  {
    return pub_ ? rcl_publisher_is_ipc_enabled(pub_->get_publisher_handle().get()) :
           rcl_subscription_is_ipc_enabled(sub_->get_subscription_handle().get());
  }
  size_t matched() {return pub_ ? pub_->get_subscription_count() : sub_->get_publisher_count();}
  void close()
  {
    if (!executor_) {return;}
    executor_->cancel();
    thread_.join();
    executor_.reset();
    pub_.reset(); sub_.reset(); node_.reset();
    context_->shutdown("component container shutdown");
    context_.reset();
  }

private:
  std::shared_ptr<rclcpp::Context> context_;
  rclcpp::Node::SharedPtr node_;
  rclcpp::Publisher<Image>::SharedPtr pub_;
  rclcpp::Subscription<Image>::SharedPtr sub_;
  std::shared_ptr<rclcpp::executors::SingleThreadedExecutor> executor_;
  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable condition_;
  uint32_t sequence_ = UINT32_MAX;
  int64_t latency_ = 0;
  bool valid_ = true;
  size_t count_ = 0;
};
PYBIND11_MODULE(_ipc_native, module)
{
  py::class_<Container>(module, "Container")
  .def(py::init<bool, bool, bool, const std::string &>())
  .def("lease", &Container::lease)
  .def("publish", &Container::publish, py::call_guard<py::gil_scoped_release>())
  .def("wait", &Container::wait, py::call_guard<py::gil_scoped_release>())
  .def("matched", &Container::matched)
  .def("enabled", &Container::enabled)
  .def("count", &Container::count)
  .def("close", &Container::close, py::call_guard<py::gil_scoped_release>());
}
