////////////////////////////////////////////////
// © https://github.com/badhitman 
////////////////////////////////////////////////

using ab.context;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using MQTTnet;
using SharedLib;
using System.Runtime.Versioning;

namespace ServerLib;

/// <summary>
/// Mqtt client service
/// </summary>
[SupportedOSPlatform("windows")]
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("android")]
public class MqttClientService : MqttBaseServiceAbstraction
{
    /// <inheritdoc/>
    public override MqttClientSubscribeOptions MqttSubscribeOptions => _mqttFactory
                .CreateSubscribeOptionsBuilder()
                .WithTopicFilter(f => { f.WithTopic(GlobalStatic.Routes.AB_LOG_SYSTEM); })
            .Build();

    /// <summary>
    /// 
    /// </summary>
    public MqttClientService(IMqttClient mqttClient, ILogger<MqttClientService> logger, MqttConfigModel mqtt_settings, MqttClientFactory mqttFactory, INotifyService notifyService, IDbContextFactory<ParametersContext> dbFactory)
        : base(mqttClient: mqttClient, mqtt_settings: mqtt_settings, mqttFactory: mqttFactory, logger: logger, notifyService: notifyService, dbFactory: dbFactory)
    {

    }

    /// <summary>
    /// 
    /// </summary>
    public override Task ApplicationMessageReceiveHandledAsync(MqttApplicationMessageReceivedEventArgs e)
    {
        return base.ApplicationMessageReceiveHandledAsync(e);
    }
}
