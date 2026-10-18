////////////////////////////////////////////////
// © https://github.com/badhitman 
////////////////////////////////////////////////

using Microsoft.Extensions.DependencyInjection;
using MQTTnet;
using MQTTnet.Adapter;
using SharedLib;
using Telegram.Bot;

namespace ServerLib;

/// <summary>
/// Tools: MQTT and Email
/// </summary>
public class ToolsLocalService(IMqttBaseService MqttClientService, IParametersStorageService ParameterStorage, MqttClientFactory MqttFact, HttpClient HttpClient, IEmailService Email, IServiceProvider ServiceProvider) : IToolsService
{
    /// <inheritdoc/>
    public async Task<ResponseBaseModel> StartMqtt(CancellationToken cancellation_token) => await MqttClientService.StartService(cancellation_token);

    /// <inheritdoc/>
    public async Task<ResponseBaseModel> StopMqtt(CancellationToken cancellation_token) => await MqttClientService.StopService(cancellation_token);

    /// <inheritdoc/>
    public Task<TResponseModel<bool>> StatusMqtt() => MqttClientService.StatusService();

    /// <inheritdoc/>
    public virtual async Task<ResponseBaseModel> TestEmailConnect(EmailConfigModel? conf = null, CancellationToken cancellation_token = default)
    {
        ResponseBaseModel res = new();
        conf ??= (await ParameterStorage.GetEmailConfig(cancellation_token)).Response;
        if (!conf!.IsConfigured)
        {
            res.AddError("Конфигурация не установлена");
            return res;
        }

        return await Email.ConnectSmtpAsync(conf);
    }

    /// <inheritdoc/>
    public async Task<ResponseBaseModel> TestMqttConnect(MqttConfigModel? conf = null, CancellationToken cancellation_token = default)
    {
        ResponseBaseModel res = new();
        conf ??= (await ParameterStorage.GetMqttConfig(cancellation_token)).Response;
        if (!conf!.IsConfigured)
        {
            res.AddError("Конфигурация не установлена");
            return res;
        }

        MqttClientOptions mqttClientOptions = new MqttClientOptionsBuilder()
#if DEBUG
           .WithTlsOptions(p => p.WithCertificateValidationHandler(sx => true))
#endif
           .WithClientId(conf.ClientId)
           .WithTcpServer(conf.Server, conf.Port)
           .WithCredentials(conf.Username, conf.Password)
           .WithCleanSession()
        .Build();
        using IMqttClient _mqtt = MqttFact.CreateMqttClient();
        try
        {
            MqttClientConnectResult connect_res = await _mqtt.ConnectAsync(mqttClientOptions, CancellationToken.None);

            if (connect_res.ResultCode != MqttClientConnectResultCode.Success)
                res.AddError(connect_res.ResultCode.ToString());
            else
            {
                res.AddSuccess($"Подключение успешно: {connect_res.ResultCode}");
                await _mqtt.DisconnectAsync(cancellationToken: cancellation_token);
            }
        }
        catch (MqttConnectingFailedException mcf)
        {
            res.AddError($"Failed to connect {mcf.Message}");
        }
        catch (Exception ex)
        {
            res.AddError($"Failed to connect {ex.Message}");
        }

        return res;
    }

    /// <inheritdoc/>
    public virtual async Task<TelegramBotCheckResponseModel> TestTelegramBotConnect(TelegramBotConfigModel? conf = null, CancellationToken cancellation_token = default)
    {
        TelegramBotCheckResponseModel res = new();
        conf ??= (await ParameterStorage.GetTelegramBotConfig(cancellation_token)).Response;

        if (string.IsNullOrEmpty(conf?.TelegramBotToken))
        {
            res.AddError("TelegramBotToken не может быть пустым");
            return res;
        }

        TelegramBotClientOptions options = new(conf.TelegramBotToken);

        try
        {
            TelegramBotClient tbot_cli = new(options, HttpClient);
            Telegram.Bot.Types.User _me = await tbot_cli.GetMe(cancellationToken: cancellation_token);

            res.FirstName = _me.FirstName;
            res.LastName = _me.LastName;
            res.Username = _me.Username;
            res.Id = _me.Id;

            ITelegramBotClient? tbc = ServiceProvider.GetService<ITelegramBotClient>();

            if (tbc is not null)
            {
                Telegram.Bot.Types.User _demon_me = await tbc.GetMe(cancellationToken: cancellation_token);
                res.ServiceIsRunning = new()
                {
                    FirstName = _demon_me.FirstName,
                    LastName = _demon_me.LastName,
                    Username = _demon_me.Username,
                    Id = _demon_me.Id
                };
            }
        }
        catch (Exception ex)
        {
            res.AddError(ex.Message);
        }

        return res;
    }

    /// <inheritdoc/>
    public async Task<MqttPublishMessageResultModel> PublishMqttMessage(MqttPublishMessageModel message, CancellationToken cancellation_token = default) => await MqttClientService.PublishMessage(message, cancellation_token);
}