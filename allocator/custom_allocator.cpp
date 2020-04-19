#include "custom_allocator.hpp"

#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <mesos/attributes.hpp>
#include <mesos/resource_quantities.hpp>
#include <mesos/resources.hpp>
#include <mesos/roles.hpp>
#include <mesos/type_utils.hpp>
#include <mesos/module.hpp>
#include <mesos/module/module.hpp>

#include <process/after.hpp>
#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/event.hpp>
#include <process/id.hpp>
#include <process/loop.hpp>
#include <process/timeout.hpp>

#include <stout/check.hpp>
#include <stout/hashset.hpp>
#include <stout/set.hpp>
#include <stout/stopwatch.hpp>
#include <stout/stringify.hpp>
#include <fstream>
#include <iostream>

using std::make_shared;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;
using std::weak_ptr;

using mesos::allocator::InverseOfferStatus;
using mesos::allocator::Options;
using mesos::allocator::Allocator;

using mesos::modules::Module;

using process::after;
using process::Continue;
using process::ControlFlow;
using process::Failure;
using process::Future;
using process::loop;
using process::Owned;
using process::PID;
using process::Timeout;

namespace mesos {
namespace modules {
namespace allocator {

class InverseOfferFilter
{
public:
  virtual ~InverseOfferFilter() {}

  virtual bool filter() const = 0;
};


// NOTE: See comment above `InverseOfferFilter` regarding capturing
// `unavailableResources` if this allocator starts sending fine-grained inverse
// offers.
class RefusedInverseOfferFilter : public InverseOfferFilter
{
public:
  RefusedInverseOfferFilter(const Duration& timeout)
    : _expired(after(timeout)) {}

  ~RefusedInverseOfferFilter() override
  {
    // Cancel the timeout upon destruction to avoid lingering timers.
    _expired.discard();
  }

  Future<Nothing> expired() const { return _expired; };

  bool filter() const override
  {
    // See comment above why we currently don't do more fine-grained filtering.
    return _expired.isPending();
  }

private:
  Future<Nothing> _expired;
};
Framework::Framework(
    const FrameworkInfo& frameworkInfo,
    bool _active)
  : frameworkId(frameworkInfo.id()),
    active(_active) {}

	void CustomAllocatorProcess::initialize(
			const mesos::allocator::Options &options,
			const lambda::function<
					void(const FrameworkID&,
							const hashmap<std::string, hashmap<SlaveID, Resources>>&)> &offerCallback,
			const lambda::function<
					void(const FrameworkID&,
							const hashmap<SlaveID, UnavailableResources>&)> &inverseOfferCallback) {
		initialized = true;
		paused = false;

	}

	void CustomAllocatorProcess::recover(const int _expectedAgentCount, const hashmap<string, Quota> &quotas) {
		  CHECK(initialized);
		  CHECK(slaves.empty());
		  CHECK(_expectedAgentCount >= 0);

		  const Duration ALLOCATION_HOLD_OFF_RECOVERY_TIMEOUT = Minutes(10);
		  const double AGENT_RECOVERY_FACTOR = 0.8;

		  expectedAgentCount =
		    static_cast<int>(_expectedAgentCount * AGENT_RECOVERY_FACTOR);

		  if (expectedAgentCount.get() == 0) {
		    VLOG(1) << "Skipping recovery of hierarchical allocator:"
		            << " no reconnecting agents to wait for";
		    return;
		  }

		  pause();
		  delay(ALLOCATION_HOLD_OFF_RECOVERY_TIMEOUT, self(), &Self::resume);
		  LOG(INFO) << "Triggered allocator recovery: waiting for "
		            << expectedAgentCount.get() << " agents to reconnect or "
		            << ALLOCATION_HOLD_OFF_RECOVERY_TIMEOUT << " to pass";
	}

	void CustomAllocatorProcess::addFramework(const FrameworkID &frameworkId,
			const FrameworkInfo &frameworkInfo,
			const hashmap<SlaveID, Resources> &used, bool active,
			const set<string> &suppressedRoles) {

		  CHECK(initialized);
		  CHECK_NOT_CONTAINS(frameworks, frameworkId);

		  frameworks.insert({frameworkId, Framework(frameworkInfo, active)});

		  LOG(INFO) << "Added framework " << frameworkId;

		  if (active) {
		    allocate();
		  } else {
		    deactivateFramework(frameworkId);
		  }
	}

	void CustomAllocatorProcess::removeFramework(const FrameworkID &frameworkId) {
		  CHECK(initialized);
		  frameworks.erase(frameworkId);
		  LOG(INFO) << "Removed framework " << frameworkId;
	}

	void CustomAllocatorProcess::activateFramework(const FrameworkID &frameworkId) {
		  CHECK(initialized);
		  Framework& framework = *CHECK_NOTNONE(getFramework(frameworkId));
		  framework.active = true;

		  LOG(INFO) << "Activated framework " << frameworkId;
		  allocate();
	}

	void CustomAllocatorProcess::deactivateFramework(const FrameworkID &frameworkId) {
		  CHECK(initialized);
		  Framework& framework = *CHECK_NOTNONE(getFramework(frameworkId));

		  framework.active = false;
		  LOG(INFO) << "Deactivated Framework " << frameworkId;
	}

	void CustomAllocatorProcess::updateFramework(const FrameworkID &frameworkId,
			const FrameworkInfo &frameworkInfo,
			const set<string> &suppressedRoles) {

		  CHECK(initialized);
		  LOG(INFO) << "Updated framework" << frameworkId;
	}

	void CustomAllocatorProcess::updateInverseOffer(
			  const SlaveID& slaveId,
			  const FrameworkID& frameworkId,
			  const Option<UnavailableResources>& unavailableResources,
			  const Option<mesos::allocator::InverseOfferStatus>& status,
			  const Option<Filters>& filters) {

		  CHECK(initialized);
		  Framework& framework = *CHECK_NOTNONE(getFramework(frameworkId));
		  Slave& slave = *CHECK_NOTNONE(getSlave(slaveId));

		  CHECK(slave.maintenance.isSome())
		    << "Agent " << slaveId
		    << " (" << slave.info.hostname() << ") should have maintenance scheduled";

		  Slave::Maintenance& maintenance = slave.maintenance.get();

		  // Only handle inverse offers that we currently have outstanding. If it is not
		  // currently outstanding this means it is old and can be safely ignored.
		  if (maintenance.offersOutstanding.contains(frameworkId)) {
		    // We always remove the outstanding offer so that we will send a new offer
		    // out the next time we schedule inverse offers.
		    maintenance.offersOutstanding.erase(frameworkId);

		    // If the response is `Some`, this means the framework responded. Otherwise
		    // if it is `None` the inverse offer timed out or was rescinded.
		    if (status.isSome()) {
		      // For now we don't allow frameworks to respond with `UNKNOWN`. The caller
		      // should guard against this. This goes against the pattern of not
		      // checking external invariants; however, the allocator and master are
		      // currently so tightly coupled that this check is valuable.
		      CHECK_NE(status->status(), InverseOfferStatus::UNKNOWN);

		      // If the framework responded, we update our state to match.
		      maintenance.statuses[frameworkId].CopyFrom(status.get());
		    }
		  }

		  if (filters.isNone()) {
		    return;
		  }

		  Try<Duration> timeout = Duration::create(Filters().refuse_seconds());

		  if (filters->refuse_seconds() > Days(365).secs()) {
		    LOG(WARNING) << "Using 365 days to create the refused inverse offer"
		                 << " filter because the input value is too big";

		    timeout = Days(365);
		  } else if (filters->refuse_seconds() < 0) {
		    LOG(WARNING) << "Using the default value of 'refuse_seconds' to create"
		                 << " the refused inverse offer filter because the input"
		                 << " value is negative";

		    timeout = Duration::create(Filters().refuse_seconds());
		  } else {
		    timeout = Duration::create(filters->refuse_seconds());

		    if (timeout.isError()) {
		      LOG(WARNING) << "Using the default value of 'refuse_seconds' to create"
		                   << " the refused inverse offer filter because the input"
		                   << " value is invalid: " + timeout.error();

		      timeout = Duration::create(Filters().refuse_seconds());
		    }
		  }

		  if (timeout.get() != Duration::zero()) {
		    VLOG(1) << "Framework " << frameworkId
		            << " filtered inverse offers from agent " << slaveId
		            << " for " << timeout.get();

		    // Create a new inverse offer filter and delay its expiration.
		    shared_ptr<RefusedInverseOfferFilter> inverseOfferFilter =
		      make_shared<RefusedInverseOfferFilter>(*timeout);

		    framework.inverseOfferFilters[slaveId].insert(inverseOfferFilter);

		    weak_ptr<InverseOfferFilter> weakPtr = inverseOfferFilter;

		    inverseOfferFilter->expired().onReady(defer(self(), [=](Nothing) {
		        expire(frameworkId, slaveId, weakPtr);
		      }));
		  }
	}

	void CustomAllocatorProcess::addSlave(const SlaveID &slaveId,
			const SlaveInfo &slaveInfo,
			const vector<SlaveInfo::Capability> &capabilities,
			const Option<Unavailability> &unavailability, const Resources &total,
			const hashmap<FrameworkID, Resources> &used) {

		  CHECK(initialized);
		  CHECK_NOT_CONTAINS(slaves, slaveId);
		  CHECK_EQ(slaveId, slaveInfo.id());
		  CHECK(!paused || expectedAgentCount.isSome());

		  slaves.insert({slaveId, Slave(slaveInfo, true, total, Resources::sum(used))});

		  Slave& slave = *CHECK_NOTNONE(getSlave(slaveId));

		  if (paused &&
		      expectedAgentCount.isSome() &&
		      (static_cast<int>(slaves.size()) >= expectedAgentCount.get())) {
		    VLOG(1) << "Recovery complete: sufficient amount of agents added; "
		            << slaves.size() << " agents known to the allocator";

		    expectedAgentCount = None();
		    resume();
		  }

		  LOG(INFO) << "Added agent " << slaveId << " (" << slave.info.hostname() << ")"
		            << " with " << slave.getTotal()
		            << " (allocated: " << slave.getAllocated() << ")";

		  allocate(slaveId);
	}

	void CustomAllocatorProcess::removeSlave(const SlaveID &slaveId) {
		CHECK(initialized);
		slaves.erase(slaveId);

		allocationCandidates.erase(slaveId);
		LOG(INFO) << "Removed agent " << slaveId;
	}

	void CustomAllocatorProcess::updateSlave(const SlaveID &slaveId,
			const SlaveInfo &info, const Option<Resources> &total,
			const Option<vector<SlaveInfo::Capability>> &capabilities) {
		  CHECK(initialized);
		  CHECK_EQ(slaveId, info.id());

		  Slave& slave = *CHECK_NOTNONE(getSlave(slaveId));
		  bool updated = false;

		  if (!(slave.info == info)) {
		    updated = true;
		    slave.info = info;
		  }
		  if (updated) {
		    allocate(slaveId);
		  }
	}

	void CustomAllocatorProcess::addResourceProvider(const SlaveID &slaveId,
			const Resources &total, const hashmap<FrameworkID, Resources> &used) {
		  CHECK(initialized);

		  Slave& slave = *CHECK_NOTNONE(getSlave(slaveId));
		  updateSlaveTotal(slaveId, slave.getTotal() + total);
		  slave.allocate(Resources::sum(used));

		  VLOG(1)
		    << "Grew agent " << slaveId << " by "
		    << total << " (total), "
		    << used << " (used)";
	}

	void CustomAllocatorProcess::activateSlave(const SlaveID &slaveId) {
		  CHECK(initialized);
		  Slave& slave = *CHECK_NOTNONE(getSlave(slaveId));
		  slave.activated = true;
		  LOG(INFO) << "Agent " << slaveId << " reactivated";
	}

	void CustomAllocatorProcess::deactivateSlave(const SlaveID &slaveId) {
		  CHECK(initialized);
		  Slave& slave = *CHECK_NOTNONE(getSlave(slaveId));
		  slave.activated = false;
		  LOG(INFO) << "Agent " << slaveId << " deactivated";
	}

	Future<hashmap<SlaveID, hashmap<FrameworkID, InverseOfferStatus>>>
	CustomAllocatorProcess::getInverseOfferStatuses()
	{
	  CHECK(initialized);

	  hashmap<SlaveID, hashmap<FrameworkID, InverseOfferStatus>> result;

	  foreachpair (const SlaveID& id, const Slave& slave, slaves) {
	    if (slave.maintenance.isSome()) {
	      result[id] = slave.maintenance->statuses;
	    }
	  }

	  return result;
	}

	void CustomAllocatorProcess::requestResources(const FrameworkID &frameworkId,
			const vector<Request> &requests) {

		  CHECK(initialized);
		  LOG(INFO) << "Received resource request from framework " << frameworkId;
	}

	void CustomAllocatorProcess::updateAllocation(const FrameworkID &frameworkId,
			const SlaveID &slaveId, const Resources &offeredResources,
			const vector<ResourceConversion> &conversions) {

		  CHECK(initialized);
		  CHECK_CONTAINS(frameworks, frameworkId);

		  Slave& slave = *CHECK_NOTNONE(getSlave(slaveId));

		  // We require that an allocation is tied to a single role.
		  //
		  // TODO(bmahler): The use of `Resources::allocations()` induces
		  // unnecessary copying of `Resources` objects (which is expensive
		  // at the time this was written).
		  hashmap<string, Resources> allocations = offeredResources.allocations();
		  CHECK_EQ(1u, allocations.size());
	}

	Future<Nothing> CustomAllocatorProcess::updateAvailable(const SlaveID &slaveId,
			const vector<Offer::Operation> &operations) {
		LOG(INFO) << "update updateAvailable called";
		return Nothing();
	}

	void CustomAllocatorProcess::updateUnavailability(const SlaveID &slaveId,
			const Option<Unavailability> &unavailability) {
		LOG(INFO) << "updateUnavailability called";
	}

	void CustomAllocatorProcess::recoverResources(const FrameworkID &frameworkId,
			const SlaveID &slaveId, const Resources &resources,
			const Option<Filters> &filters) {
		LOG(INFO) << "recoverResources called";
	}

	void CustomAllocatorProcess::suppressOffers(const FrameworkID &frameworkId,
			const set<string> &roles_) {
		LOG(INFO) << "suppressOffers called";
	}

	void CustomAllocatorProcess::reviveOffers(const FrameworkID &frameworkId,
			const set<string> &roles) {
		LOG(INFO) << "reviveOffers called";
	}


	void CustomAllocatorProcess::pause() {
		LOG(INFO) << "Allocation Paused";
	}

	void CustomAllocatorProcess::resume() {
		LOG(INFO) << "Allocation resumed";
	}

	//MARK:- PROTECTED METHODS

	//MARK:- PRIVATE METHODS

	Option<Slave*> CustomAllocatorProcess::getSlave(const SlaveID& slaveId) const {
		  auto it = slaves.find(slaveId);
		  if (it == slaves.end()) return None();
		  return const_cast<Slave*>(&it->second);
	}

	Option<Framework*> CustomAllocatorProcess::getFramework(const FrameworkID& frameworkId) const {
		  auto it = frameworks.find(frameworkId);
		  if (it == frameworks.end()) return None();
		  return const_cast<Framework*>(&it->second);
	}

	bool CustomAllocatorProcess::updateSlaveTotal(const SlaveID& slaveId, const Resources& total) {

		  Slave& slave = *CHECK_NOTNONE(getSlave(slaveId));
		  const Resources oldTotal = slave.getTotal();
		  if (oldTotal == total) {
		    return false;
		  }
		  slave.updateTotal(total);
		  return true;
	}

	void CustomAllocatorProcess::expire(const FrameworkID& frameworkId, const SlaveID& slaveId, const weak_ptr<InverseOfferFilter>& inverseOfferFilter) {
		// The filter might have already been removed (e.g., if the
		// framework no longer exists or in
		// HierarchicalAllocatorProcess::reviveOffers) but
		// we may land here if the cancelation of the expiry timeout
		// did not succeed (due to the dispatch already being in the
		// queue).
		shared_ptr<InverseOfferFilter> filter = inverseOfferFilter.lock();

		if (filter.get() == nullptr) {
		return;
		}

		// Since this is a performance-sensitive piece of code,
		// we use find to avoid the doing any redundant lookups.
		auto frameworkIterator = frameworks.find(frameworkId);
		CHECK(frameworkIterator != frameworks.end());

		Framework& framework = frameworkIterator->second;

		auto filters = framework.inverseOfferFilters.find(slaveId);
		CHECK(filters != framework.inverseOfferFilters.end());

		filters->second.erase(filter);
		if (filters->second.empty()) {
		framework.inverseOfferFilters.erase(slaveId);
		}
	}

}
}
}
