# --- Build stage ---
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Cache restore
COPY IoTIngestion.csproj .
RUN dotnet restore

# Build
COPY . .
RUN dotnet publish -c Release -o /app /p:UseAppHost=false

# --- Runtime stage ---
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .

ENV ASPNETCORE_URLS=http://0.0.0.0:9091
ENV METRICS_PORT=9091
EXPOSE 9091

ENTRYPOINT ["dotnet", "IoTIngestion.dll"]
