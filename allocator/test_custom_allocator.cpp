#include <mesos/allocator/allocator.hpp>
#include <mesos/module/allocator.hpp>
#include <stout/try.hpp>
#include <iostream>

#include "custom_allocator.hpp"

using namespace mesos;
using mesos::allocator::Allocator;
using mesos::modules::allocator::CustomAllocator;

static Allocator* createExternalAllocator(const Parameters &parameters) {

	std::cout << "---Create External Allocator Called---\n";
	Try<Allocator*> allocator = CustomAllocator::create();
	if (allocator.isError()) {
		return nullptr;
	}

	return allocator.get();
}

mesos::modules::Module<Allocator> CustomAllocatorModule(
MESOS_MODULE_API_VERSION,
MESOS_VERSION, "Mesos Contributor", "engineer@example.com", "CustomAllocator",
		nullptr, createExternalAllocator);
