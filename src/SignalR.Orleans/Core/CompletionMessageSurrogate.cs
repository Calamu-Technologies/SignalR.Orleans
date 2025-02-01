using Microsoft.AspNetCore.SignalR.Protocol;

namespace SignalR.Orleans.Core;

[GenerateSerializer, Immutable]
public readonly struct CompletionMessageSurrogate
{
    [Id(0)]
    public readonly string? InvocationId;

    [Id(1)]
    public readonly string? Error;

    [Id(2)]
    public readonly object? Result;

    [Id(3)]
    public readonly bool HasResult;

    public CompletionMessageSurrogate(string? invocationId, string? error, object? result, bool hasResult)
    {
        InvocationId = invocationId;
        Error = error;
        Result = result;
        HasResult = hasResult;
    }
}

[RegisterConverter]
public sealed class CompletionMessageSurrogateConverter : IConverter<CompletionMessage, CompletionMessageSurrogate>
{
    public CompletionMessage ConvertFromSurrogate(in CompletionMessageSurrogate surrogate)
    {
        return new CompletionMessage(
            invocationId: surrogate.InvocationId!,
            error: surrogate.Error,
            result: surrogate.Result,
            hasResult: surrogate.HasResult);
    }

    public CompletionMessageSurrogate ConvertToSurrogate(in CompletionMessage value)
    {
        return new CompletionMessageSurrogate(
            invocationId: value.InvocationId,
            error: value.Error,
            result: value.Result,
            hasResult: value.HasResult);
    }
}
