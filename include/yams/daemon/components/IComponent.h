#pragma once

#include <yams/core/types.h>

namespace yams::daemon {

/**
 * @class IComponent
 * @brief An interface for all active components within the daemon.
 *
 * This provides a common lifecycle for components that have initialization
 * and shutdown procedures.
 */
class IComponent {
public:
    virtual ~IComponent() = default;

    /**
     * @brief Returns the name of the component for logging and identification.
     */
    virtual const char* getName() const = 0;

    /**
     * @brief Initializes the component. This can be a long-running operation.
     * @return Result<void> indicating success or failure.
     */
    virtual Result<void> initialize() = 0;

    /**
     * @brief Shuts down the component and releases its resources.
     */
    virtual void shutdown() = 0;
};

} // namespace yams::daemon
