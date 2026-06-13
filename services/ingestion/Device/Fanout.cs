namespace IoTIngestion.Device;

/// <summary>
/// Računa fanout: 20 pilota × N replika = NUM_DEVICES.
/// DeviceId = "{pilot}-{replica}" (npr. "5-42" = pilot 5, replika 42).
/// </summary>
public static class Fanout
{
    public const int PilotCount = 20;

    public readonly record struct VirtualDevice(int PilotIndex, int Replica)
    {
        public string DeviceId => $"{PilotIndex}-{Replica}";
    }

    public static IEnumerable<VirtualDevice> BuildDevices(int numDevices)
    {
        if (numDevices < 1) throw new ArgumentOutOfRangeException(nameof(numDevices));
        if (numDevices < PilotCount)
        {
            // Manje uređaja nego pilota — svaki uređaj = 1 pilot, replike samo za prvih
            int i = 0;
            for (int p = 0; p < numDevices; p++)
                yield return new VirtualDevice(p, 0);
            i++;
        }
        else
        {
            int perPilot = numDevices / PilotCount;
            int remainder = numDevices % PilotCount;
            for (int p = 0; p < PilotCount; p++)
            {
                int replicas = perPilot + (p < remainder ? 1 : 0);
                for (int r = 0; r < replicas; r++)
                    yield return new VirtualDevice(p, r);
            }
        }
    }
}
