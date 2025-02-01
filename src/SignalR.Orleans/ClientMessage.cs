using Microsoft.AspNetCore.SignalR.Protocol;

namespace SignalR.Orleans;

[Immutable, GenerateSerializer]
public sealed record ClientMessage(string HubName, string ConnectionId, Guid fromServerId, [Immutable] InvocationMessage Message);

[Immutable, GenerateSerializer]
public sealed record ClientResultMessage(string HubName, string ConnectionId, Guid fromServerId, [Immutable] CompletionMessage Message);
