using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using IoTIngestion.Payloads;

namespace IoTIngestion.Replay;

/// <summary>
/// Streaming reader za Data.csv. Čita jedan red u memoriji (F1 telemetrija ima
/// 56 kolona, red je ~500 bajtova). Za 1.1M redova, ceo fajl je ~500MB RAM
/// ako se učita odjednom, ali streaming čuva konstantnu memoriju.
/// </summary>
public sealed class CsvReader
{
    public sealed class CsvRow
    {
        // Mapiranje kolona iz Data.csv
        public float SessionTime { get; set; }
        public int FrameIdentifier { get; set; }
        public int PilotIndex { get; set; }
        public float WorldPositionX { get; set; }
        public float WorldPositionY { get; set; }
        public float WorldPositionZ { get; set; }
        public float Speed { get; set; }
        public float EngineTemperature { get; set; }
        public float TyresSurfaceTemperature { get; set; }
    }

    private readonly string _path;
    private readonly ILogger _logger;

    public CsvReader(string path, ILogger logger)
    {
        _path = path;
        _logger = logger;
    }

    /// <summary>
    /// Enumeriše redove iz CSV-a. Zatvara reader kad se enumeracija završi.
    /// </summary>
    public IEnumerable<CsvRow> Read()
    {
        if (!File.Exists(_path))
        {
            _logger.LogError("Data.csv ne postoji na putanji {Path}", _path);
            throw new FileNotFoundException($"Data.csv not found: {_path}", _path);
        }

        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            TrimOptions = TrimOptions.Trim,
            MissingFieldFound = null,    // CSV ima neke prazne kolone (npr. pitStatus)
            BadDataFound = null
        };

        using var reader = new StreamReader(_path);
        using var csv = new CsvHelper.CsvReader(reader, config);

        // Očigledno mapiranje po imenu
        csv.Context.RegisterClassMap<CsvRowMap>();

        foreach (var row in csv.GetRecords<CsvRow>())
        {
            yield return row;
        }
    }

    private sealed class CsvRowMap : ClassMap<CsvRow>
    {
        public CsvRowMap()
        {
            Map(m => m.SessionTime).Name("sessionTime");
            Map(m => m.FrameIdentifier).Name("frameIdentifier");
            Map(m => m.PilotIndex).Name("pilot_index");
            Map(m => m.WorldPositionX).Name("worldPositionX");
            Map(m => m.WorldPositionY).Name("worldPositionY");
            Map(m => m.WorldPositionZ).Name("worldPositionZ");
            Map(m => m.Speed).Name("speed");
            Map(m => m.EngineTemperature).Name("engineTemperature");
            Map(m => m.TyresSurfaceTemperature).Name("tyresSurfaceTemperature");
        }
    }
}
