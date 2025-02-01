using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Orleans.Streams;
using SignalR.Orleans.Core;
using Orleans;
using System.Reflection;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Streams.Core;

namespace SignalR.Orleans;

// TODO: Is this thing called in a threadsafe manner by signalR? 
public partial class OrleansHubLifetimeManager<THub> : HubLifetimeManager<THub>, ILifecycleParticipant<ISiloLifecycle>,
    IDisposable, IAsyncDisposable where THub : Hub
{
    private readonly Guid _serverId;
    private readonly ILogger _logger;
    private readonly string _hubName;
    private IClusterClient? _clusterClient;
    private readonly SemaphoreSlim _streamSetupLock = new(1);
    private readonly HubConnectionStore _connections = new();
    private readonly ClientResultsManager _clientResultsManager = new();
    private readonly IServiceProvider _serviceProvider;

    private IStreamProvider? _streamProvider = null;
    private IAsyncStream<ClientMessage> _serverStream = default!;
    private IAsyncStream<ClientResultMessage> _serverResultStream = default!;
    private IAsyncStream<AllMessage> _allStream = default!;
    StreamSubscriptionHandle<AllMessage> _allStreamSubscription =  default!;
    StreamSubscriptionHandle<ClientMessage> _serverStreamSubscription = default!;
    StreamSubscriptionHandle<ClientResultMessage> _serverResultStreamSubscription = default!;
    private Timer _timer = default!;

    public OrleansHubLifetimeManager(
        IServiceProvider serviceProvider,
        ILogger<OrleansHubLifetimeManager<THub>> logger,
        IClusterClient clusterClient
    )
    {
        var hubType = typeof(THub).BaseType?.GenericTypeArguments.FirstOrDefault() ?? typeof(THub);
        _hubName = hubType.IsInterface && hubType.Name[0] == 'I'
            ? hubType.Name[1..]
            : hubType.Name;
        _serverId = Guid.NewGuid();
        _logger = logger;
        _clusterClient = clusterClient;
        _serviceProvider = serviceProvider;
    }

    private Task HeartbeatCheck()
    {
        if (_clusterClient is null) return Task.CompletedTask;
        return _clusterClient.GetServerDirectoryGrain().Heartbeat(_serverId);
    }

    private async Task EnsureStreamSetup()
    {
        if (_streamProvider is not null)
            return;

        await _streamSetupLock.WaitAsync();

        try
        {
            if (_streamProvider is not null)
                return;

            _logger.LogInformation(
                "Initializing: Orleans HubLifetimeManager {hubName} (serverId: {serverId})...",
                _hubName, _serverId);

            _streamProvider = _clusterClient!.GetOrleansSignalRStreamProvider();
            _serverStream = _streamProvider.GetServerStream(_serverId);
            _serverResultStream = _streamProvider.GetServerResultStream(_serverId);
            _allStream = _streamProvider.GetAllStream(_hubName);

            _timer = new Timer(
                _ => Task.Run(HeartbeatCheck), null, TimeSpan.FromSeconds(0),
                TimeSpan.FromMinutes(SignalROrleansConstants.SERVER_HEARTBEAT_PULSE_IN_MINUTES));

            _allStreamSubscription = await _allStream.SubscribeAsync( (msg, sequenceToken) => ProcessAllMessage(msg, sequenceToken));
            _logger.LogInformation(
                "Orleans HubLifetimeManager {hubName} - ALL_STREAM Subscription Handle {handleId} (serverId: {serverId})",
                _hubName, _allStreamSubscription.HandleId.ToString(), _serverId);
            
            _serverStreamSubscription = await _serverStream.SubscribeAsync((msg, sequenceToken) => ProcessServerMessage(msg, sequenceToken));
            _logger.LogInformation(
                "Orleans HubLifetimeManager {hubName} - SERVER_STREAM Subscription Handle {handleId} (serverId: {serverId})",
                _hubName, _serverStreamSubscription.HandleId.ToString(), _serverId);

            _serverResultStreamSubscription = await _serverResultStream.SubscribeAsync((msg, sequenceToken) => ProcessServerResultMessage(msg, sequenceToken));
            _logger.LogInformation(
                "Orleans HubLifetimeManager {hubName} - SERVER_RESULT_STREAM Subscription Handle {handleId} (serverId: {serverId})",
                _hubName, _serverStreamSubscription.HandleId.ToString(), _serverId);


            var subManager = _serviceProvider.GetService<IStreamSubscriptionManagerAdmin>()!
                            .GetStreamSubscriptionManager(StreamSubscriptionManagerType.ExplicitSubscribeOnly);
            if (subManager is not null)
            {
                var subscriptions = await subManager.GetSubscriptions(SignalROrleansConstants.SIGNALR_ORLEANS_STREAM_PROVIDER, StreamId.Create("SERVER_STREAM", _serverId));
                foreach (var sub in subscriptions)
                    _logger.LogInformation("subscriptions seen by server {serverId} - {sub}", _serverId, sub.ToString());
            }

            _logger.LogInformation(
                "Initialized complete: Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
                _hubName, _serverId);
        }
        finally
        {
            _streamSetupLock.Release();
        }
    }

    private Task ProcessAllMessage(AllMessage allMessage, StreamSequenceToken sequenceToken)
    {
        var allTasks = new List<Task>(_connections.Count);
        var payload = allMessage.Message!;

        foreach (var connection in _connections)
        {
            if (connection.ConnectionAborted.IsCancellationRequested)
                continue;

            if (allMessage.ExcludedIds == null || !allMessage.ExcludedIds.Contains(connection.ConnectionId))
                allTasks.Add(SendLocal(connection, payload));
        }

        return Task.WhenAll(allTasks);
    }

    private Task ProcessServerMessage(ClientMessage clientMessage, StreamSequenceToken sequenceToken)
    {
        var connection = _connections[clientMessage.ConnectionId];
        if (connection == null)
        {
            _logger.LogDebug("ProcessServerMessage - connection {connectionId} on hub {hubName} not connected to (serverId: {serverId})",
                clientMessage.ConnectionId, _hubName, _serverId);
            return Task.CompletedTask;
        }
        else
        {
            _logger.LogDebug("ProcessServerMessage - connection {connectionId} on hub {hubName} (serverId: {serverId})",
                clientMessage.ConnectionId, _hubName, _serverId);
        }

        using var _ = CancellationTokenUtils.CreateLinkedToken(CancellationToken.None,
            connection?.ConnectionAborted ?? CancellationToken.None, out var linkedToken);
        var task = _clientResultsManager.AddInvocation<THub>(clientMessage.ConnectionId, clientMessage.fromServerId, clientMessage.Message.InvocationId!, linkedToken);

        try
        {
            var message = new InvocationMessage(clientMessage.Message.InvocationId!, clientMessage.Message.Target, clientMessage.Message.Arguments);
            return SendLocal(connection!, message);
        }
        catch
        {
            _clientResultsManager.RemoveInvocation(clientMessage.Message.InvocationId!);
            return Task.CompletedTask;
        }
    }

    private Task ProcessServerResultMessage(ClientResultMessage clientResultMessage, StreamSequenceToken sequenceToken)
    {
        var connection = _connections[clientResultMessage.ConnectionId];
        if (connection == null)
        {
            _logger.LogDebug("ProcessServerResultMessage - connection {connectionId} on hub {hubName} not connected to (serverId: {serverId})",
                clientResultMessage.ConnectionId, _hubName, _serverId);
        }
        else
        {
            _logger.LogDebug("ProcessServerResultMessage - connection {connectionId} on hub {hubName} (serverId: {serverId})",
                clientResultMessage.ConnectionId, _hubName, _serverId);
        }

        return connection == null ? Task.CompletedTask : SetConnectionResultAsync(clientResultMessage.ConnectionId, clientResultMessage.Message);
    }

    public override async Task OnConnectedAsync(HubConnectionContext connection)
    {
        await EnsureStreamSetup();

        try
        {
            if (_clusterClient is null)
            {
                _logger.LogError("Invalid clusterClient");
                return;
            }

            _logger.LogDebug("Handle connection {connectionId} on hub {hubName} (serverId: {serverId})",
                connection.ConnectionId, _hubName, _serverId);

            _connections.Add(connection);

            var client = _clusterClient.GetClientGrain(_hubName, connection.ConnectionId);
            await client.OnConnect(_serverId);

            if (connection!.User!.Identity!.IsAuthenticated)
            {
                var user = _clusterClient.GetUserGrain(_hubName, connection.UserIdentifier!);
                if (user is not null)
                    await user.Add(connection.ConnectionId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "An error has occurred 'OnConnectedAsync' while adding connection {connectionId} [hub: {hubName} (serverId: {serverId})]",
                connection?.ConnectionId, _hubName, _serverId);
            _connections.Remove(connection!);
            throw;
        }
    }

    public override async Task OnDisconnectedAsync(HubConnectionContext connection)
    {
        try
        {
            if (_clusterClient is null) 
            {
                _logger.LogError("Invalid clusterClient");
                return; 
            }

            _logger.LogDebug("Handle disconnection {connectionId} on hub {hubName} (serverId: {serverId})",
                connection.ConnectionId, _hubName, _serverId);
            var client = _clusterClient.GetClientGrain(_hubName, connection.ConnectionId);
            await client.OnDisconnect("hub-disconnect");
        }
        finally
        {
            _connections.Remove(connection);
        }
    }

    public override Task SendAllAsync(string methodName, object?[] args, CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("SendAllAsync - MethodName: {methodName}", methodName);
        var message = new InvocationMessage(methodName, args);
        return _allStream.OnNextAsync(new AllMessage(message));
    }

    public override Task SendAllExceptAsync(string methodName, object?[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("SendAllExceptAsync - MethodName: {methodName}, ExcludedConnectionIds: {excludedConnectionIds}", methodName, excludedConnectionIds);
        var message = new InvocationMessage(methodName, args);
        return _allStream.OnNextAsync(new AllMessage(message, excludedConnectionIds));
    }

    public override Task SendConnectionAsync(string connectionId, string methodName, object?[] args, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(connectionId)) throw new ArgumentNullException(nameof(connectionId));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var message = new InvocationMessage(methodName, args);

        var connection = _connections[connectionId];
        if (connection != null) return SendLocal(connection, message);

        return SendExternal(connectionId, message);
    }

    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object?[] args, CancellationToken cancellationToken = default)
    {
        var tasks = connectionIds.Select(c => SendConnectionAsync(c, methodName, args, cancellationToken));
        return Task.WhenAll(tasks);
    }

    public override Task SendGroupAsync(string groupName, string methodName, object?[] args, CancellationToken cancellationToken = default)
    {
        if (_clusterClient is null) return Task.CompletedTask;

        if (string.IsNullOrWhiteSpace(groupName)) throw new ArgumentNullException(nameof(groupName));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.Send(_serverId, methodName, args);
    }

    public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object?[] args, CancellationToken cancellationToken = default)
    {
        var tasks = groupNames.Select(g => SendGroupAsync(g, methodName, args, cancellationToken));
        return Task.WhenAll(tasks);
    }

    public override Task SendGroupExceptAsync(string groupName, string methodName, object?[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default)
    {
        if (_clusterClient is null) return Task.CompletedTask;

        if (string.IsNullOrWhiteSpace(groupName)) throw new ArgumentNullException(nameof(groupName));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.SendExcept(_serverId, methodName, args, excludedConnectionIds);
    }

    public override Task SendUserAsync(string userId, string methodName, object?[] args, CancellationToken cancellationToken = default)
    {
        if (_clusterClient is null) return Task.CompletedTask;

        if (string.IsNullOrWhiteSpace(userId)) throw new ArgumentNullException(nameof(userId));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var user = _clusterClient.GetUserGrain(_hubName, userId);
        return user.Send(_serverId, methodName, args);
    }

    public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object?[] args, CancellationToken cancellationToken = default)
    {
        var tasks = userIds.Select(u => SendGroupAsync(u, methodName, args, cancellationToken));
        return Task.WhenAll(tasks);
    }

    public override Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        if (_clusterClient is null) return Task.CompletedTask;

        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.Add(connectionId);
    }

    public override Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        if (_clusterClient is null) return Task.CompletedTask;

        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.Remove(connectionId);
    }

    private Task SendLocal(HubConnectionContext connection, InvocationMessage hubMessage)
    {
        _logger.LogDebug(
            "Sending local message to connection {connectionId} on hub {hubName} (serverId: {serverId})",
            connection.ConnectionId, _hubName, _serverId);

        return connection.WriteAsync(hubMessage).AsTask();
    }

    private Task SendExternal(string connectionId, InvocationMessage hubMessage)
    {
        _logger.LogDebug(
            "Send external message to connection {connectionId} on hub {hubName} (serverId: {serverId})",
            connectionId, _hubName, _serverId);

        if (_clusterClient is null)
        {
            _logger.LogError("Invalid clusterClient");
            return Task.CompletedTask;
        }

        var client = _clusterClient.GetClientGrain(_hubName, connectionId);
        return client.Send(_serverId, hubMessage);
    }

    public override async Task<T> InvokeConnectionAsync<T>(string connectionId, string methodName, object?[] args,
        CancellationToken cancellationToken)
    {
        // validate
        if (string.IsNullOrWhiteSpace(connectionId)) throw new ArgumentNullException(nameof(connectionId));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));
        
        // try to get the connection from this silo connection store
        var connection = _connections[connectionId];
        
        // Generate cluster-unique identifier for the invocation.
        // ID needs to be unique for each invocation and across servers, we generate a GUID every time, that should provide enough uniqueness guarantees.
        var invocationId = Guid.NewGuid().ToString();

        _logger.LogDebug("InvokeConnectionAsync {connectionId} on hub {hubName} {methodName} invocation {invocationId} (serverId: {serverId})",
                connectionId, _hubName, methodName, invocationId, _serverId);

        using var _ = CancellationTokenUtils.CreateLinkedToken(cancellationToken,
            connection?.ConnectionAborted ?? CancellationToken.None, out var linkedToken);
        var task = _clientResultsManager.AddInvocation<T>(connectionId, _serverId, invocationId, linkedToken);

        try
        {
            var message = new InvocationMessage(invocationId, methodName, args);
            if (connection == null)
            {
                await SendExternal(connectionId, message);
            }
            else
            {
                await SendLocal(connection, message);
            }
        }
        catch
        {
            _clientResultsManager.RemoveInvocation(invocationId);
            throw;
        }

        try
        {
            return await task;
        }
        catch
        {
            // ConnectionAborted will trigger a generic "Canceled" exception from the task, let's convert it into a more specific message.
            if (connection?.ConnectionAborted.IsCancellationRequested == true)
            {
                throw new IOException($"Connection '{connectionId}' disconnected.");
            }

            throw;
        }
    }

    public override Task SetConnectionResultAsync(string connectionId, CompletionMessage result)
    {
        // We have to complete the local invocation first
        var fromServerId = _clientResultsManager.TryCompleteResult(connectionId, result);

        _logger.LogDebug(
                "SetConnectionResultAsync for connection {connectionId} on hub {hubName} (serverId: {serverId}) from {fromServerId} result: {result}",
                connectionId, _hubName, _serverId, fromServerId, result.ToString());

        // If not empty and doesn't equal this server's id, send the results to the original server to
        // complete that invocation also.
        if (fromServerId != Guid.Empty && fromServerId != _serverId)
        {
            _logger.LogDebug(
                "Send external result message to connection {connectionId} on hub {hubName} (serverId: {serverId})",
                connectionId, _hubName, _serverId);

            if (_clusterClient is null)
            {
                _logger.LogError("Invalid clusterClient");
                return Task.CompletedTask;
            }

            var client = _clusterClient.GetClientGrain(_hubName, connectionId);
            return client.SendResult(_serverId, result);
        }

        return Task.CompletedTask;
    }

    public override bool TryGetReturnType(string invocationId, [NotNullWhen(true)] out Type? type)
    {
        return _clientResultsManager.TryGetType(invocationId, out type);
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore();
        _clusterClient = null;
        Dispose(false);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        // sync disposables
        if (disposing)
        {
            _timer?.Dispose();
            _timer = null!;
            _clusterClient = null;
        }

        // no async disposables are sync compatible on this class
    }

    protected virtual async ValueTask DisposeAsyncCore()
    {
        _timer?.Dispose();
        _timer = null!;

        // Note: We can't UnsubscribeAsync these because by the time we get
        // here, the backing objects in the cluster are already disposed.
        /*var toUnsubscribe = new List<Task>();
        if (_serverStream is not null)
        {
            var serverStream = _serverStream;

            toUnsubscribe.Add(Task.Run(async () =>
            {
                IEnumerable<StreamSubscriptionHandle<ClientMessage>> subscriptions;
                try
                {
                    subscriptions = await serverStream.GetAllSubscriptionHandles();
                }
                catch
                {
                    return;
                }
                try
                {
                    await Task.WhenAll(subscriptions.Select(s => s.UnsubscribeAsync()));
                }
                catch
                {
                    return;
                }
            }));

            _serverStream = null!;
        }

        if (_allStream is not null)
        {
            var allStream = _allStream;

            toUnsubscribe.Add(Task.Run(async () =>
            {
                IEnumerable<StreamSubscriptionHandle<AllMessage>> subscriptions;
                try
                {
                    subscriptions = await allStream.GetAllSubscriptionHandles();
                }
                catch
                {
                    return;
                }
                try
                {
                    await Task.WhenAll(subscriptions.Select(s => s.UnsubscribeAsync()));
                }
                catch
                {
                    return;
                }
            }));

            _allStream = null!;
        }
        
        if (_clusterClient  is not null)
        { 
            toUnsubscribe.Add(Task.Run(async () =>
            {
                try
                {
                    await _clusterClient.GetServerDirectoryGrain().Unregister(_serverId);
                }
                catch
                {
                    return;
                }
            }));

            try
            {
                await Task.WhenAll(toUnsubscribe);
            }
            catch
            {
                // noop
            }
        }
        */
        _clusterClient = null;
        await Task.CompletedTask;
    }

    public void Participate(ISiloLifecycle lifecycle)
    {
        lifecycle.Subscribe(
           observerName: nameof(OrleansHubLifetimeManager<THub>),
           stage: ServiceLifecycleStage.Active,
           onStart: async cts => await Task.Run(EnsureStreamSetup, cts));
    }
}
