using System.Text.Json.Serialization;

namespace IoTIngestion.Payloads;

/// <summary>
/// Pojedinačni IoT telemetrijski event. Oblik JSON-a mora biti stabilan
/// jer ga Storage i Analytics čitaju (isti shape za oba brokera).
/// </summary>
public sealed class TelemetryEvent
{
    [JsonPropertyName("device_id")]
    public string DeviceId { get; set; } = string.Empty;

    [JsonPropertyName("pilot_index")]
    public int PilotIndex { get; set; }

    [JsonPropertyName("replica")]
    public int Replica { get; set; }

    [JsonPropertyName("sessionTime")]
    public float SessionTime { get; set; }

    [JsonPropertyName("frameIdentifier")]
    public int FrameIdentifier { get; set; }

    [JsonPropertyName("speed")]
    public float Speed { get; set; }

    [JsonPropertyName("engineTemperature")]
    public float EngineTemperature { get; set; }

    [JsonPropertyName("tyresSurfaceTemperature")]
    public float TyresSurfaceTemperature { get; set; }

    [JsonPropertyName("worldPositionX")]
    public float WorldPositionX { get; set; }

    [JsonPropertyName("worldPositionY")]
    public float WorldPositionY { get; set; }

    [JsonPropertyName("worldPositionZ")]
    public float WorldPositionZ { get; set; }

    [JsonPropertyName("t_emit")]
    public long TEmit { get; set; }
}
