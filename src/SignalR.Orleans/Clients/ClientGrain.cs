using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Concurrency;
using SignalR.Orleans.Core;
using Orleans.Streams.Core;
using Microsoft.Extensions.DependencyInjection;

namespace SignalR.Orleans.Clients;

/// <inheritdoc cref="IClientGrain"/>
internal sealed class ClientGrain : IGrainBase, IClientGrain
{
    private const string CLIENT_STORAGE = "ClientState";
    private const int MAX_FAIL_ATTEMPTS = 1;

    private readonly ILogger<ClientGrain> _logger;
    private readonly IPersistentState<ClientGrainState> _clientState;

    private string _hubName = default!;
    private string _connectionId = default!;
    private Guid _serverId => _clientState.State.ServerId;

    public IGrainContext GrainContext { get; }

    private IStreamProvider _streamProvider = default!;
    private StreamSubscriptionHandle<Guid>? _serverDisconnectedSubscription = default;
    IAsyncStream<ClientMessage> _serverStream = default!;
    IAsyncStream<ClientResultMessage> _serverResultStream = default!;
    private readonly IServiceProvider _serviceProvider;

    private int _failAttempts = 0;

    public ClientGrain(
        IServiceProvider serviceProvider,
        ILogger<ClientGrain> logger,
        IGrainContext grainContext,
        [PersistentState(CLIENT_STORAGE, SignalROrleansConstants.SIGNALR_ORLEANS_STORAGE_PROVIDER)] IPersistentState<ClientGrainState> clientState, IOptions<InternalOptions> options)
    {
        _logger = logger;
        _clientState = options.Value.ConflateStorageAccess ? clientState.WithConflation() : clientState;
        this.GrainContext = grainContext;
        _serviceProvider = serviceProvider;
    }

    public async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var key = ClientKey.FromGrainPrimaryKey(this.GetPrimaryKeyString());
        _hubName = key.HubType;
        _connectionId = key.ConnectionId;
        _streamProvider = this.GetOrleansSignalRStreamProvider();

        // Resume subscriptions if we have already been "connected".
        // We know we have already been connected if the "ServerId" parameter is set.
        if (_serverId != default)
        {
            // We will listen to this stream to know if the server is disconnected (silo goes down) so that we can enact client disconnected procedure.
            var serverDisconnectedStream = _streamProvider.GetServerDisconnectionStream(_clientState.State.ServerId);
            var _serverDisconnectedSubscription = (await serverDisconnectedStream.GetAllSubscriptionHandles())[0];
            await _serverDisconnectedSubscription.ResumeAsync((serverId, _) => OnDisconnect("server-disconnected"));

            _serverStream = _streamProvider.GetServerStream(_clientState.State.ServerId);
            _serverResultStream = _streamProvider.GetServerResultStream(_clientState.State.ServerId);

            _logger.LogDebug("ResumeAsync {serverId}", _clientState.State.ServerId);
        }
    }

    public async Task OnConnect(Guid serverId)
    {
        var serverDisconnectedStream = _streamProvider.GetServerDisconnectionStream(serverId);
        _serverDisconnectedSubscription = await serverDisconnectedStream.SubscribeAsync(_ => OnDisconnect("server-disconnected"));

        _serverStream = _streamProvider.GetServerStream(serverId);
        _serverResultStream = _streamProvider.GetServerResultStream(serverId);

        _clientState.State.ServerId = serverId;
        _logger.LogDebug("ClientGrain.OnConnect - ConnectionId {connectionId} on Server {serverId}", _connectionId, serverId);

        await _clientState.WriteStateAsync();
    }

    public async Task OnDisconnect(string? reason = null)
    {
        _logger.LogDebug("ClientGrain Disconnecting connection on {hubName} for connection {connectionId} from server {serverId} via reason '{reason}'.",
            _hubName, _connectionId, _clientState.State.ServerId, reason);

        if (_serverDisconnectedSubscription is not null)
        {
            await _serverDisconnectedSubscription.UnsubscribeAsync();
            _serverDisconnectedSubscription = null;
        }

        await _streamProvider.GetClientDisconnectionStream(_connectionId).OnNextAsync(_connectionId);

        _clientState.State.ServerId = default;

        await _clientState.ClearStateAsync();

        this.DeactivateOnIdle();
    }

    // NB: Interface method is marked [ReadOnly] so this method will be re-entrant/interleaved.
    public async Task Send(Guid fromServerId, [Immutable] InvocationMessage message)
    {
        var subManager = _serviceProvider.GetService<IStreamSubscriptionManagerAdmin>()!
                            .GetStreamSubscriptionManager(StreamSubscriptionManagerType.ExplicitSubscribeOnly);
        if (subManager is not null)
        {
            var subscriptions = await subManager.GetSubscriptions(SignalROrleansConstants.SIGNALR_ORLEANS_STREAM_PROVIDER, StreamId.Create("SERVER_STREAM", _serverId));
            foreach (var sub in subscriptions)
            {
                _logger.LogInformation("Subscriptions seen by server {serverId}, {grainId}, {providerName}, {streamId}, {subscriptionId}",
                    _clientState.State.ServerId, sub.GrainId, sub.StreamProviderName, sub.StreamId, sub.SubscriptionId);
            }
        }

        if (_serverStream != default)
        {
            _logger.LogDebug("ClientGrain Sending message on stream to {hubName}.{target} to connection {connectionId} server {serverId} fromServer {fromServerId}",
                _hubName, message.Target, _connectionId, _clientState.State.ServerId, fromServerId);

            // Routes the message to the silo (server) where the client is actually connected.
            await _serverStream.OnNextAsync(new ClientMessage(_hubName, _connectionId, fromServerId, message));

            Interlocked.Exchange(ref _failAttempts, 0);
        }
        else
        {
            _logger.LogInformation("Client not connected for connectionId '{connectionId}' and hub '{hubName}' ({targetMethod})",
                _connectionId, _hubName, message.Target);

            if (Interlocked.Increment(ref _failAttempts) >= MAX_FAIL_ATTEMPTS)
            {
                _logger.LogWarning("Force disconnect client for connectionId {connectionId} and hub {hubName} ({targetMethod}) after exceeding attempts limit",
                    _connectionId, _hubName, message.Target);

                await OnDisconnect("attempts-limit-reached");
            }
        }
    }

    public async Task SendResult(Guid fromServerId, [Immutable] CompletionMessage message)
    {
        if (_serverResultStream != default)
        {
            _logger.LogDebug("Sending results message on stream to {hubName} invocation {invocationId} to connection {connectionId}",
                _hubName, message.InvocationId, _connectionId);

            // Routes the message to the silo (server) where the client is actually connected.
            await _serverResultStream.OnNextAsync(new ClientResultMessage(_hubName, _connectionId, fromServerId, message));

            Interlocked.Exchange(ref _failAttempts, 0);
        }
        else
        {
            _logger.LogInformation("Client not connected for connectionId '{connectionId}' and hub '{hubName}' ({invocationId})",
                _connectionId, _hubName, message.InvocationId);

            if (Interlocked.Increment(ref _failAttempts) >= MAX_FAIL_ATTEMPTS)
            {
                _logger.LogWarning("Force disconnect client for connectionId {connectionId} and hub {hubName} ({invocationId}) after exceeding attempts limit",
                    _connectionId, _hubName, message.InvocationId);

                await OnDisconnect("attempts-limit-reached");
            }
        }
    }

    public Task SendOneWay(Guid fromServerId, InvocationMessage message) => Send(fromServerId, message);
}