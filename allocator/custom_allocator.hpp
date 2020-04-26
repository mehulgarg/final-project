#include<mesos/mesos.hpp>

#ifndef __TEST_ALLOCATOR_MESOS_PARTITION_HPP__
#define __TEST_ALLOCATOR_MESOS_PARTITION_HPP__

#include <memory>
#include <set>
#include <string>

#include <mesos/mesos.hpp>
#include <mesos/allocator/allocator.hpp>
#include <mesos/module/allocator.hpp>
#include "allocator.hpp"

#include <process/future.hpp>
#include <process/id.hpp>
#include <process/owned.hpp>

#include <stout/boundedhashmap.hpp>
#include <stout/duration.hpp>
#include <stout/hashmap.hpp>
#include <stout/hashset.hpp>
#include <stout/lambda.hpp>
#include <stout/option.hpp>

using namespace mesos::internal::master::allocator;

namespace mesos {
namespace modules {
namespace allocator {

class InverseOfferFilter;

struct Framework {
	Framework(const FrameworkInfo &frameworkInfo, bool active);

	const FrameworkID frameworkId;

	bool active;

	  hashmap<SlaveID, hashset<std::shared_ptr<InverseOfferFilter>>> inverseOfferFilters;
};

class Slave
{
public:
  Slave(
      const SlaveInfo& _info,
      bool _activated,
      const Resources& _total,
      const Resources& _allocated)
    : info(_info),
      activated(_activated),
      total(_total),
      allocated(_allocated),
      shared(_total.shared()),
      hasGpu_(_total.gpus().getOrElse(0) > 0)
  {
    updateAvailable();
  }

  const Resources& getTotal() const { return total; }

  const Resources& getAllocated() const { return allocated; }

  const Resources& getAvailable() const { return available; }

  bool hasGpu() const { return hasGpu_; }

  void updateTotal(const Resources& newTotal) {
    total = newTotal;
    shared = total.shared();
    hasGpu_ = total.gpus().getOrElse(0) > 0;

    updateAvailable();
  }

  void allocate(const Resources& toAllocate)
  {
    allocated += toAllocate;

    updateAvailable();
  }

  void unallocate(const Resources& toUnallocate)
  {
    allocated -= toUnallocate;

    updateAvailable();
  }

  // The `SlaveInfo` that was passed to the allocator when the slave was added
  // or updated. Currently only two fields are used: `hostname` for host
  // white-listing and in log messages, and `domain` for region-aware
  // scheduling.
  SlaveInfo info;

  bool activated; // Whether to offer resources.

  // Represents a scheduled unavailability due to maintenance for a specific
  // slave, and the responses from frameworks as to whether they will be able
  // to gracefully handle this unavailability.
  //
  // NOTE: We currently implement maintenance in the allocator to be able to
  // leverage state and features such as the FrameworkSorter and OfferFilter.
  struct Maintenance
  {
    Maintenance(const Unavailability& _unavailability)
      : unavailability(_unavailability) {}

    // The start time and optional duration of the event.
    Unavailability unavailability;

    // A mapping of frameworks to the inverse offer status associated with
    // this unavailability.
    //
    // NOTE: We currently lose this information during a master fail over
    // since it is not persisted or replicated. This is okay as the new master's
    // allocator will send out new inverse offers and re-collect the
    // information. This is similar to all the outstanding offers from an old
    // master being invalidated, and new offers being sent out.
    hashmap<FrameworkID, mesos::allocator::InverseOfferStatus> statuses;

    // Represents the "unit of accounting" for maintenance. When a
    // `FrameworkID` is present in the hash-set it means an inverse offer has
    // been sent out. When it is not present it means no offer is currently
    // outstanding.
    hashset<FrameworkID> offersOutstanding;
  };

  // When the `maintenance` is set the slave is scheduled to be unavailable at
  // a given point in time, for an optional duration. This information is used
  // to send out `InverseOffers`.
  Option<Maintenance> maintenance;

private:
  void updateAvailable() {
    // In order to subtract from the total,
    // we strip the allocation information.
    Resources allocated_ = allocated;
    allocated_.unallocate();

    // Calling `nonShared()` currently copies the underlying resources
    // and is therefore rather expensive. We avoid it in the common
    // case that there are no shared resources.
    if (shared.empty()) {
      available = total - allocated_;
    } else {
      // Since shared resources are offer-able even when they are in use, we
      // always include them as part of available resources.
      available = (total.nonShared() - allocated_.nonShared()) + shared;
    }
  }

  // Total amount of regular *and* over-subscribed resources.
  Resources total;

  // Regular *and* over-subscribed resources that are allocated.
  //
  // NOTE: We maintain multiple copies of each shared resource allocated
  // to a slave, where the number of copies represents the number of times
  // this shared resource has been allocated to (and has not been recovered
  // from) a specific framework.
  //
  // NOTE: We keep track of the slave's allocated resources despite
  // having that information in sorters. This is because the
  // information in sorters is not accurate if some framework
  // hasn't re-registered. See MESOS-2919 for details.
  Resources allocated;

  // We track the total and allocated resources on the slave to
  // avoid calculating it in place every time.
  //
  // Note that `available` always contains all the shared resources on the
  // agent regardless whether they have ever been allocated or not.
  // NOTE, however, we currently only offer a shared resource only if it has
  // not been offered in an allocation cycle to a framework. We do this mainly
  // to preserve the normal offer behavior. This may change in the future
  // depending on use cases.
  //
  // Note that it's possible for the slave to be over-allocated!
  // In this case, allocated > total.
  Resources available;

  Resources shared;

  bool hasGpu_;
};

class CustomAllocatorProcess;

typedef MesosAllocator<CustomAllocatorProcess> CustomAllocator;
class CustomAllocatorProcess: public MesosAllocatorProcess {
public:

	~CustomAllocatorProcess() override {
	}

	process::PID<CustomAllocatorProcess> self() const {
		return process::PID<Self>(this);
	}

	void initialize(const mesos::allocator::Options &options,
			const lambda::function<
					void(const FrameworkID&,
							const hashmap<std::string,
									hashmap<SlaveID, Resources>>&)> &offerCallback,
			const lambda::function<
					void(const FrameworkID&,
							const hashmap<SlaveID, UnavailableResources>&)> &inverseOfferCallback)
					override;

	void recover(const int _expectedAgentCount,
			const hashmap<std::string, Quota> &quotas) override;

	void addFramework(const FrameworkID &frameworkId,
			const FrameworkInfo &frameworkInfo,
			const hashmap<SlaveID, Resources> &used, bool active,
			const std::set<std::string> &suppressedRoles) override;

	void removeFramework(const FrameworkID &frameworkId) override;

	void activateFramework(const FrameworkID &frameworkId) override;

	void deactivateFramework(const FrameworkID &frameworkId) override;

	void updateFramework(const FrameworkID &frameworkId,
			const FrameworkInfo &frameworkInfo,
			const std::set<std::string> &suppressedRoles) override;

	void addSlave(const SlaveID &slaveId, const SlaveInfo &slaveInfo,
			const std::vector<SlaveInfo::Capability> &capabilities,
			const Option<Unavailability> &unavailability,
			const Resources &total, const hashmap<FrameworkID, Resources> &used)
					override;
	  void updateWhitelist(
	      const Option<hashset<std::string>>& whitelist) override;

	  process::Future<
	      hashmap<SlaveID,
	      hashmap<FrameworkID, mesos::allocator::InverseOfferStatus>>>
	    getInverseOfferStatuses() override;

	void removeSlave(const SlaveID &slaveId) override;

	void updateSlave(const SlaveID &slave, const SlaveInfo &slaveInfo,
			const Option<Resources> &total = None(),
			const Option<std::vector<SlaveInfo::Capability>> &capabilities =
					None()) override;

	void addResourceProvider(const SlaveID &slave, const Resources &total,
			const hashmap<FrameworkID, Resources> &used) override;

	void deactivateSlave(const SlaveID &slaveId) override;

	void activateSlave(const SlaveID &slaveId) override;

	void updateInverseOffer(
	  const SlaveID& slaveId,
	  const FrameworkID& frameworkId,
	  const Option<UnavailableResources>& unavailableResources,
	  const Option<mesos::allocator::InverseOfferStatus>& status,
	  const Option<Filters>& filters) override;

	void requestResources(const FrameworkID &frameworkId,
			const std::vector<Request> &requests) override;

	void updateAllocation(const FrameworkID &frameworkId,
			const SlaveID &slaveId, const Resources &offeredResources,
			const std::vector<ResourceConversion> &conversions) override;

	process::Future<Nothing> updateAvailable(const SlaveID &slaveId,
			const std::vector<Offer::Operation> &operations) override;

	void updateUnavailability(const SlaveID &slaveId,
			const Option<Unavailability> &unavailability) override;

	void recoverResources(const FrameworkID &frameworkId,
			const SlaveID &slaveId, const Resources &resources,
			const Option<Filters> &filters) override;

	void suppressOffers(const FrameworkID &frameworkId,
			const std::set<std::string> &roles) override;

	void reviveOffers(const FrameworkID &frameworkId,
			const std::set<std::string> &roles) override;

	  void updateQuota(
	      const std::string& role,
	      const Quota& quota) override;

	  void updateWeights(
	      const std::vector<WeightInfo>& weightInfos) override;

	void pause() override;

	void resume() override;

protected:

	typedef CustomAllocatorProcess Self;
	typedef CustomAllocatorProcess This;

	hashmap<FrameworkID, Framework> frameworks;

	hashmap<std::string, std::set<SlaveID>> partitionToSlave;
	SlaveID decoy_slaveId;

	hashmap<SlaveID, std::string> slaveToPartition;

	hashmap<FrameworkID, std::string> allocatedFrameworks;

	Option<process::Future<Nothing>> allocation;

	hashset<SlaveID> allocationCandidates;

	mesos::allocator::Options options;

	hashmap<SlaveID, Slave> slaves;

	Option<int> expectedAgentCount;

	process::Future<Nothing> allocate();

	// Allocate resources from the specified agent.
	process::Future<Nothing> allocate(const SlaveID& slaveId);

	// Allocate resources from the specified agents. The allocation
	// is deferred and batched with other allocation requests.
	process::Future<Nothing> allocate(const hashset<SlaveID>& slaveIds);

private:

	bool initialized;

	bool paused;

	struct constants {
		const std::string red = "\033[0;31m";
		const std::string green = "\033[0;32m";
		const std::string yellow = "\033[0;33m";
		const std::string blue = "\033[0;34m";
		const std::string reset = "\033[0m";
	};

	struct constants Constants;

	Option<Slave*> getSlave(const SlaveID& slaveId) const;
	Option<Framework*> getFramework(const FrameworkID& frameworkId) const;

	void expire(const FrameworkID& frameworkId, const SlaveID& slaveId, const std::weak_ptr<InverseOfferFilter>& inverseOfferFilter);

	bool updateSlaveTotal(const SlaveID& slaveId, const Resources& total);

	void getPartitionData();

};

}
}
}

#endif
