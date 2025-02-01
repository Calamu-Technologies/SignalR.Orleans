// Attribution from: https://source.dot.net/#Microsoft.AspNetCore.SignalR.StackExchangeRedis/src/SignalR/common/Shared/ClientResultsManager.cs
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
 
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace SignalR.Orleans.Core;
 
// Common type used by our HubLifetimeManager implementations to manage client results.
// Handles cancellation, cleanup, and completion, so any bugs or improvements can be made in a single place
internal sealed class ClientResultsManager : IInvocationBinder
{
    private readonly ConcurrentDictionary<string, (Type Type, string ConnectionId, Guid fromServerId, object Tcs, Action<object, CompletionMessage> Complete)> _pendingInvocations = new();
 
    public Task<T> AddInvocation<T>(string connectionId, Guid fromServerId, string invocationId, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSourceWithCancellation<T>(this, connectionId, fromServerId, invocationId, cancellationToken);
        var result = _pendingInvocations.TryAdd(invocationId, (typeof(T), connectionId, fromServerId, tcs, static (state, completionMessage) =>
        {
            var tcs = (TaskCompletionSourceWithCancellation<T>)state;
            if (completionMessage.HasResult)
            {
                tcs.SetResult((T)completionMessage.Result!);
            }
            else
            {
                tcs.SetException(new HubException(completionMessage.Error));
            }
        }
        ));
        Debug.Assert(result);
 
        tcs.RegisterCancellation();
 
        return tcs.Task;
    }
 
    public void AddInvocation(string invocationId, (Type Type, string ConnectionId, Guid fromServerId, object Tcs, Action<object, CompletionMessage> Complete) invocationInfo)
    {
        var result = _pendingInvocations.TryAdd(invocationId, invocationInfo);
        Debug.Assert(result);
        // Should have a 50% chance of happening once every 2.71 quintillion invocations (see UUID in Wikipedia)
        if (!result)
        {
            invocationInfo.Complete(invocationInfo.Tcs, CompletionMessage.WithError(invocationId, "ID collision occurred when using client results. This is likely a bug in SignalR."));
        }
    }

    /// <summary>
    /// Sends the results back to the caller.
    /// </summary>
    /// <param name="connectionId">The connection to send the result to.</param>
    /// <param name="message">The results message to send.</param>
    /// <returns>Returns a Guid of the server that originally received the message, or Guid.Empty if the Invocation has already completed.</returns>
    /// <exception cref="InvalidOperationException"></exception>
    public Guid TryCompleteResult(string connectionId, CompletionMessage message)
    {
        if (_pendingInvocations.TryGetValue(message.InvocationId!, out var item))
        {
            if (item.ConnectionId != connectionId)
            {
                throw new InvalidOperationException($"Connection ID '{connectionId}' is not valid for invocation ID '{message.InvocationId}'.");
            }
 
            // if false the connection disconnected right after the above TryGetValue
            // or someone else completed the invocation (likely a bad client)
            // we'll ignore both cases
            if (_pendingInvocations.Remove(message.InvocationId!, out _))
            {
                item.Complete(item.Tcs, message);
                return item.fromServerId;
            }
        }
        else
        {
            // connection was disconnected or someone else completed the invocation
        }

        return Guid.Empty;
    }
 
    public (Type Type, string ConnectionId, Guid fromServerId, object Tcs, Action<object, CompletionMessage> Completion)? RemoveInvocation(string invocationId)
    {
        _pendingInvocations.TryRemove(invocationId, out var item);
        return item;
    }
 
    public bool TryGetType(string invocationId, [NotNullWhen(true)] out Type? type)
    {
        if (_pendingInvocations.TryGetValue(invocationId, out var item))
        {
            type = item.Type;
            return true;
        }
        type = null;
        return false;
    }
 
    public Type GetReturnType(string invocationId)
    {
        if (TryGetType(invocationId, out var type))
        {
            return type;
        }
        throw new InvalidOperationException($"Invocation ID '{invocationId}' is not associated with a pending client result.");
    }
 
    // Unused, here to honor the IInvocationBinder interface but should never be called
    public IReadOnlyList<Type> GetParameterTypes(string methodName)
    {
        throw new NotImplementedException();
    }
 
    // Unused, here to honor the IInvocationBinder interface but should never be called
    public Type GetStreamItemType(string streamId)
    {
        throw new NotImplementedException();
    }
 
    // Custom TCS type to avoid the extra allocation that would be introduced if we managed the cancellation separately
    // Also makes it easier to keep track of the CancellationTokenRegistration for disposal
    internal sealed class TaskCompletionSourceWithCancellation<T> : TaskCompletionSource<T>
    {
        private readonly ClientResultsManager _clientResultsManager;
        private readonly string _connectionId;
        private readonly Guid _fromServerId;
        private readonly string _invocationId;
        private readonly CancellationToken _token;
 
        private CancellationTokenRegistration _tokenRegistration;
 
        public TaskCompletionSourceWithCancellation(ClientResultsManager clientResultsManager, string connectionId, Guid fromServerId, string invocationId,
            CancellationToken cancellationToken)
            : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            _clientResultsManager = clientResultsManager;
            _connectionId = connectionId;
            _fromServerId = fromServerId;
            _invocationId = invocationId;
            _token = cancellationToken;
        }
 
        // Needs to be called after adding the completion to the dictionary in order to avoid synchronous completions of the token registration
        // not canceling when the dictionary hasn't been updated yet.
        public void RegisterCancellation()
        {
            if (_token.CanBeCanceled)
            {
                _tokenRegistration = _token.UnsafeRegister(static o =>
                {
                    var tcs = (TaskCompletionSourceWithCancellation<T>)o!;
                    tcs.SetCanceled();
                }, this);
            }
        }
 
        public new void SetCanceled()
        {
            // TODO: OrleansHubLifetimeManager will want to notify the other server (if there is one) about the cancellation
            // so it can clean up state and potentially forward that info to the connection
            _clientResultsManager.TryCompleteResult(_connectionId, CompletionMessage.WithError(_invocationId, "Invocation canceled by the server."));
        }
 
        public new void SetResult(T result)
        {
            _tokenRegistration.Dispose();
            base.SetResult(result);
        }
 
        public new void SetException(Exception exception)
        {
            _tokenRegistration.Dispose();
            base.SetException(exception);
        }
 
#pragma warning disable IDE0060 // Remove unused parameter
        // Just making sure we don't accidentally call one of these without knowing
        public static new void SetCanceled(CancellationToken cancellationToken) => Debug.Assert(false);
        public static new void SetException(IEnumerable<Exception> exceptions) => Debug.Assert(false);
        public static new bool TrySetCanceled()
        {
            Debug.Assert(false);
            return false;
        }
        public static new bool TrySetCanceled(CancellationToken cancellationToken)
        {
            Debug.Assert(false);
            return false;
        }
        public static new bool TrySetException(IEnumerable<Exception> exceptions)
        {
            Debug.Assert(false);
            return false;
        }
        public static new bool TrySetException(Exception exception)
        {
            Debug.Assert(false);
            return false;
        }
        public static new bool TrySetResult(T result)
        {
            Debug.Assert(false);
            return false;
        }
#pragma warning restore IDE0060 // Remove unused parameter
    }
}