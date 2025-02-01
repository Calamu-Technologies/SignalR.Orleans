using Microsoft.AspNetCore.SignalR.Protocol;
using Orleans.Runtime;
using Orleans.Concurrency;

namespace SignalR.Orleans.Core;

/// <summary>
/// Represents an object that can invoke hub methods on a single connection.
/// </summary>
public interface IHubMessageInvoker : IAddressable
{
    /// <summary>
    /// Invokes a method on the hub.
    /// </summary>
    /// <param name="fromServerId">The serverId sending this message.</param>
    /// <param name="message">Message to invoke.</param>
    [ReadOnly] // Allows re-entrancy on this method
    Task Send(Guid fromServerId, InvocationMessage message);

    /// <summary>
    /// Send invocation results to the hub.
    /// </summary>
    /// <param name="fromServerId">The serverId sending this message.</param>
    /// <param name="message">Message Result to send.</param>
    [ReadOnly] // Allows re-entrancy on this method
    Task SendResult(Guid fromServerId, CompletionMessage message);

    [OneWay]
    Task SendOneWay(Guid fromServerId, InvocationMessage message);
}
