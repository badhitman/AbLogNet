////////////////////////////////////////////////
// © https://github.com/badhitman 
////////////////////////////////////////////////

using ab.context;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;
using Newtonsoft.Json;
using SharedLib;
using System.Buffers;
using System.Runtime.Versioning;
using System.Text;
using Telegram.Bot;

namespace ServerLib;

/// <summary>
/// 
/// </summary>
[SupportedOSPlatform("windows")]
[SupportedOSPlatform("android")]
[SupportedOSPlatform("linux")]
public class MqttServerService : MqttBaseServiceAbstraction
{
    readonly ISystemCommandsService _sys_com_service;
    readonly IHardwiresService _hardwires_service;
    readonly IServiceProvider _service_provider;
    readonly IUsersService _users_service;
    readonly IServiceCollection _services;
    readonly HttpClient _http_client;
    readonly IEmailService _email;
    readonly IHostApplicationLifetime _applicationLifetime;

    readonly MqttConfigModel _mqtt_conf;

    /// <inheritdoc/>
    public override MqttClientSubscribeOptions MqttSubscribeOptions => _mqttFactory
                .CreateSubscribeOptionsBuilder()
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.AB_LOG_SYSTEM}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Cameras}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.SHOT}"); })

                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardwires}/{GlobalStatic.Routes.HTTP}"); })
                //
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardwires}/{GlobalStatic.Routes.LIST}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardware}/{GlobalStatic.Routes.GET}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardware}/{GlobalStatic.Routes.ENTRIES}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardware}/{GlobalStatic.Routes.NESTED_ENTRIES}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardware}/{GlobalStatic.Routes.UPDATE}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardware}/{GlobalStatic.Routes.DELETE}"); })

                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Port}/{GlobalStatic.Routes.GET}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Port}/{GlobalStatic.Routes.CHECK}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Port}/{GlobalStatic.Routes.UPDATE}"); })

                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Storage}/{GlobalStatic.Routes.TelegramBot}/{GlobalStatic.Routes.GET}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Storage}/{GlobalStatic.Routes.TelegramBot}/{GlobalStatic.Routes.UPDATE}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Tools}/{GlobalStatic.Routes.TelegramBot}/{GlobalStatic.Routes.CHECK}"); })

                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Storage}/{GlobalStatic.Routes.Email}/{GlobalStatic.Routes.GET}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Storage}/{GlobalStatic.Routes.Email}/{GlobalStatic.Routes.UPDATE}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Tools}/{GlobalStatic.Routes.Email}/{GlobalStatic.Routes.CHECK}"); })

                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Users}/{GlobalStatic.Routes.GET}/{GlobalStatic.Routes.LIST}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Users}/{GlobalStatic.Routes.GET}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Users}/{GlobalStatic.Routes.UPDATE}"); })

                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.System}/{GlobalStatic.Routes.Commands}/{GlobalStatic.Routes.LIST}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.System}/{GlobalStatic.Routes.Commands}/{GlobalStatic.Routes.DELETE}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.System}/{GlobalStatic.Routes.Commands}/{GlobalStatic.Routes.UPDATE}"); })
                .WithTopicFilter(f => { f.WithTopic($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.System}/{GlobalStatic.Routes.Commands}/{GlobalStatic.Routes.START}"); })
            .Build();

    /// <summary>
    /// 
    /// </summary>
    public MqttServerService(IMqttClient mqttClient, IServiceCollection services, MqttConfigModel mqtt_conf, ISystemCommandsService sys_com_service, IUsersService users_service, HttpClient http_client, ILogger<MqttServerService> logger, MqttConfigModel mqtt_settings, MqttClientFactory mqttFactory, IHardwiresService hardwires_service, IEmailService email, IServiceProvider service_provider, INotifyService notifyService, IDbContextFactory<ParametersContext> dbFactory, IHostApplicationLifetime applicationLifetime)
        : base(mqttClient: mqttClient, mqtt_settings: mqtt_settings, mqttFactory: mqttFactory, logger: logger, notifyService: notifyService, dbFactory: dbFactory)
    {
        _logger = logger;
        _hardwires_service = hardwires_service;
        _http_client = http_client;
        _services = services;
        _sys_com_service = sys_com_service;
        _users_service = users_service;
        _email = email;
        _service_provider = service_provider;
        _mqtt_conf = mqtt_conf;
        _applicationLifetime = applicationLifetime;
    }

    /// <inheritdoc/>
    public override async Task ApplicationMessageReceiveHandledAsync(MqttApplicationMessageReceivedEventArgs e)
    {
        await base.ApplicationMessageReceiveHandledAsync(e);
        _logger.LogInformation($"call >> {nameof(ApplicationMessageReceiveHandledAsync)}");
        byte[] payload_bytes = await CipherService.DecryptAsync(e.ApplicationMessage.Payload.ToArray(), _mqtt_settings.Secret ?? CipherService.DefaultSecret, e.ApplicationMessage.CorrelationData);

        string payload_json = Encoding.UTF8.GetString(payload_bytes);
        string salt = Guid.NewGuid().ToString();

        PortHardwareCheckRequestModel? req_port_check;
        TResponseModel<TelegramBotConfigModel> tbc_res;
        TResponseModel<EmailConfigModel> ec_res;
        HardwareGetHttpRequestModel? req_http;
        SystemCommandModelDB? req_sys_com_db;
        TelegramBotConfigModel? req_conf_tbot;
        EmailConfigModel? req_conf_email;
        UserListGetModel? req_users_list;
        UpdateUserModel? req_user_upd;
        SimpleIdNoiseModel? req_nid;
        LongIdNoiseModel? req_nlid;
        HardwareBaseModel? req_hw;
        NoiseModel? req_noise;
        EntryModel? req_e;

        if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardwires}/{GlobalStatic.Routes.HTTP}"))
        {
            req_http = JsonConvert.DeserializeObject<HardwareGetHttpRequestModel>(payload_json);
            if (req_http is null)
            {
                _logger.LogError("req is null. error 1EB8300D-9436-44B0-9413-31BAA01018F2");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. error 1EB8300D-9436-44B0-9413-31BAA01018F2")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _hardwires_service.GetHardwareHtmlPage(req_http)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }

        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardwires}/{GlobalStatic.Routes.LIST}"))
        {
            req_noise = JsonConvert.DeserializeObject<NoiseModel>(payload_json);
            if (req_noise is null)
            {
                _logger.LogError("req is null. error 1C55124E-8DF4-4CCC-A9FF-8CC2C0AF6B65");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. error 1C55124E-8DF4-4CCC-A9FF-8CC2C0AF6B65")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _hardwires_service.HardwiresGetAll()), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardwires}/{GlobalStatic.Routes.ENTRIES}"))
        {
            req_noise = JsonConvert.DeserializeObject<NoiseModel>(payload_json);
            if (req_noise is null)
            {
                _logger.LogError("req is null. error C1595114-C3E5-4AE1-937B-8B45C59C8507");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. error C1595114-C3E5-4AE1-937B-8B45C59C8507")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _hardwires_service.HardwiresGetAllAsEntries()), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardwires}/{GlobalStatic.Routes.NESTED_ENTRIES}"))
        {
            req_noise = JsonConvert.DeserializeObject<NoiseModel>(payload_json);
            if (req_noise is null)
            {
                _logger.LogError("req is null. error 787DE141-F012-4369-AD88-95ACBB7BD937");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. error 787DE141-F012-4369-AD88-95ACBB7BD937")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _hardwires_service.HardwiresGetTreeNestedEntries()), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }

        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardware}/{GlobalStatic.Routes.GET}"))
        {
            req_nid = JsonConvert.DeserializeObject<SimpleIdNoiseModel>(payload_json);
            if (req_nid is null)
            {
                _logger.LogError("req is null. error 98C12EAC-73A9-4946-8850-7B075613833E");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. error 98C12EAC-73A9-4946-8850-7B075613833E")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _hardwires_service.HardwareGet(req_nid.Id)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardware}/{GlobalStatic.Routes.UPDATE}"))
        {
            req_hw = JsonConvert.DeserializeObject<HardwareBaseModel>(payload_json);
            if (req_hw is null)
            {
                _logger.LogError("req is null. error F7657C6A-2601-49BE-8F65-F302F5B1B4A3");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. error F7657C6A-2601-49BE-8F65-F302F5B1B4A3")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _hardwires_service.HardwareUpdate(req_hw)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Hardware}/{GlobalStatic.Routes.DELETE}"))
        {
            req_nid = JsonConvert.DeserializeObject<SimpleIdNoiseModel>(payload_json);
            if (req_nid is null)
            {
                _logger.LogError("req is null. error 86375673-BAA9-41EA-83F9-0F30DB918D05");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. error 86375673-BAA9-41EA-83F9-0F30DB918D05")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _hardwires_service.HardwareDelete(req_nid.Id)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }

        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Port}/{GlobalStatic.Routes.GET}"))
        {
            req_nid = JsonConvert.DeserializeObject<SimpleIdNoiseModel>(payload_json);
            if (req_nid is null)
            {
                _logger.LogError("req is null. error D4D48AE6-C24E-4532-AA92-07ECB1AFB549");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. error D4D48AE6-C24E-4532-AA92-07ECB1AFB549")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _hardwires_service.HardwarePortGet(req_nid.Id)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Port}/{GlobalStatic.Routes.CHECK}"))
        {
            req_port_check = JsonConvert.DeserializeObject<PortHardwareCheckRequestModel>(payload_json);
            if (req_port_check is null)
            {
                _logger.LogError("req is null. error 2CB7CA8A-18B3-4FDF-84F0-CD8BBEA4BA45");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. error 2CB7CA8A-18B3-4FDF-84F0-CD8BBEA4BA45")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _hardwires_service.CheckPortHardware(req_port_check)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Port}/{GlobalStatic.Routes.UPDATE}"))
        {
            req_e = JsonConvert.DeserializeObject<EntryModel>(payload_json);
            if (req_e is null)
            {
                _logger.LogError("req is null. 2415B593-DAD5-4B61-849F-7B95A174739F");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. 2415B593-DAD5-4B61-849F-7B95A174739F")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _hardwires_service.SetNamePort(req_e)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }

        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Storage}/{GlobalStatic.Routes.TelegramBot}/{GlobalStatic.Routes.GET}"))
        {
            req_noise = JsonConvert.DeserializeObject<NoiseModel>(payload_json);
            if (req_noise is null)
            {
                _logger.LogError("req is null. C39195BC-C94E-442C-9724-3FABEF157648");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. C39195BC-C94E-442C-9724-3FABEF157648")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            tbc_res = new();
            try
            {
                using ParametersContext _context = await _dbFactory.CreateDbContextAsync();
                string _telegramBotConfig = _context.GetStoredParameter(nameof(TelegramBotConfigModel), "").StoredValue;
                if (string.IsNullOrWhiteSpace(_telegramBotConfig))
                {
                    tbc_res.AddError("Конфигурация не обнаружена error {BCE8ADDA-0A51-4B91-9751-A215015CF415}");
                    await PublishMessage(JsonConvert.SerializeObject(tbc_res), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                    return;
                }
                tbc_res.Response = JsonConvert.DeserializeObject<TelegramBotConfigModel>(_telegramBotConfig);
                await PublishMessage(JsonConvert.SerializeObject(tbc_res), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }
            catch (Exception ex)
            {
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError($"{ex} error {{763C9703-3D51-405B-AB09-B47DF584A0DE}}")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Tools}/{GlobalStatic.Routes.TelegramBot}/{GlobalStatic.Routes.CHECK}"))
        {
            req_conf_tbot = JsonConvert.DeserializeObject<TelegramBotConfigModel>(payload_json);
            if (req_conf_tbot is null)
            {
                _logger.LogError("req is null. 6814E514-AB2A-495E-93C7-B2E71787338F");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. 6814E514-AB2A-495E-93C7-B2E71787338F")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }

            TelegramBotCheckResponseModel res = new();

            if (string.IsNullOrEmpty(req_conf_tbot?.TelegramBotToken))
            {
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("TelegramBotToken не может быть пустым")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }

            TelegramBotClientOptions options = new(req_conf_tbot.TelegramBotToken);

            try
            {
                TelegramBotClient tbot_cli = new(options, _http_client);
                Telegram.Bot.Types.User _me = await tbot_cli.GetMe();
                res.FirstName = _me.FirstName;
                res.LastName = _me.LastName;
                res.Username = _me.Username;
                res.Id = _me.Id;

                ITelegramBotClient? tbc = _service_provider.GetService<ITelegramBotClient>();
                if (tbc is not null)
                {
                    Telegram.Bot.Types.User _demon_me = await tbc.GetMe();
                    res.ServiceIsRunning = new()
                    {
                        FirstName = _demon_me.FirstName,
                        LastName = _demon_me.LastName,
                        Username = _demon_me.Username,
                        Id = _demon_me.Id
                    };
                }
                await PublishMessage(JsonConvert.SerializeObject(res), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }
            catch (Exception ex)
            {
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError(ex.Message)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Storage}/{GlobalStatic.Routes.TelegramBot}/{GlobalStatic.Routes.UPDATE}"))
        {
            req_conf_tbot = JsonConvert.DeserializeObject<TelegramBotConfigModel>(payload_json);
            if (req_conf_tbot is null)
            {
                _logger.LogError("req is null. 720D6445-7D3F-4BE7-AF9E-0C57D48DB5AC");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. 720D6445-7D3F-4BE7-AF9E-0C57D48DB5AC")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }

            try
            {
                using ParametersContext _context = await _dbFactory.CreateDbContextAsync();
                ParametersStorageModelDB p = _context.SetStoredParameter(nameof(TelegramBotConfigModel), JsonConvert.SerializeObject(req_conf_tbot));
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateSuccess("Данные успешно сохранены")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }
            catch (Exception ex)
            {
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError($"{ex.Message} // error {{9A97464F-6A50-4CE2-912E-0B0F6B6761BD}}")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }
        }

        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Storage}/{GlobalStatic.Routes.Email}/{GlobalStatic.Routes.GET}"))
        {
            req_noise = JsonConvert.DeserializeObject<NoiseModel>(payload_json);
            if (req_noise is null)
            {
                _logger.LogError("req is null. 1EC03B3C-645D-4F9C-A0FC-199522559D50");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. 1EC03B3C-645D-4F9C-A0FC-199522559D50")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            ec_res = new();
            try
            {
                using ParametersContext _context = await _dbFactory.CreateDbContextAsync();
                string _emailConfig = _context.GetStoredParameter(nameof(EmailConfigModel), "").StoredValue;
                if (string.IsNullOrWhiteSpace(_emailConfig))
                {
                    ec_res.AddError("Конфигурация не обнаружена error {03DD8049-0772-44D1-BDBB-C25AA690F62B}");
                    await PublishMessage(JsonConvert.SerializeObject(ec_res), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                    return;
                }
                ec_res.Response = JsonConvert.DeserializeObject<EmailConfigModel>(_emailConfig);
                await PublishMessage(JsonConvert.SerializeObject(ec_res), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }
            catch (Exception ex)
            {
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError($"{ex} error {{A0B806CF-BD77-40D5-AC5A-38EEDD2FCB7F}}")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Tools}/{GlobalStatic.Routes.Email}/{GlobalStatic.Routes.CHECK}"))
        {
            req_conf_email = JsonConvert.DeserializeObject<EmailConfigModel>(payload_json);
            if (req_conf_email is null)
            {
                _logger.LogError("req is null. 6814E514-AB2A-495E-93C7-B2E71787338F");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. 02232606-6FE8-4A7D-9F3D-DF6F7C77349D")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }

            if (!req_conf_email.IsConfigured)
            {
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("Email конфигурация не заполнена")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }

            try
            {
                ResponseBaseModel e_cli = await _email.ConnectSmtpAsync(req_conf_email);
                await PublishMessage(JsonConvert.SerializeObject(e_cli), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }
            catch (Exception ex)
            {
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError(ex.Message)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Storage}/{GlobalStatic.Routes.Email}/{GlobalStatic.Routes.UPDATE}"))
        {
            req_conf_email = JsonConvert.DeserializeObject<EmailConfigModel>(payload_json);
            if (req_conf_email is null)
            {
                _logger.LogError("req_conf_email is null. 75046E18-6180-47DB-A8D5-F0D0AF67F35A");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. 75046E18-6180-47DB-A8D5-F0D0AF67F35A")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }

            try
            {
                using ParametersContext _context = await _dbFactory.CreateDbContextAsync();
                ParametersStorageModelDB p = _context.SetStoredParameter(nameof(EmailConfigModel), JsonConvert.SerializeObject(req_conf_email));
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateSuccess("Данные успешно сохранены")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }
            catch (Exception ex)
            {
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError($"{ex.Message} // error {{17D29B87-313F-46FF-9E57-3D8167653046}}")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
            }

        }

        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.System}/{GlobalStatic.Routes.Commands}/{GlobalStatic.Routes.LIST}"))
        {
            req_noise = JsonConvert.DeserializeObject<NoiseModel>(payload_json);
            if (req_noise is null)
            {
                _logger.LogError("req is null. error A10621EE-505C-4384-8D58-F0BBE7159492");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req is null. error A10621EE-505C-4384-8D58-F0BBE7159492")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _sys_com_service.CommandsGetAll()), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.System}/{GlobalStatic.Routes.Commands}/{GlobalStatic.Routes.UPDATE}"))
        {
            req_sys_com_db = JsonConvert.DeserializeObject<SystemCommandModelDB>(payload_json);
            if (req_sys_com_db is null)
            {
                _logger.LogError("req_sys_com_bd is null. error 23C49BF0-411D-4511-8006-5C4241D83EED");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req_sys_com_bd is null. error 23C49BF0-411D-4511-8006-5C4241D83EED")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _sys_com_service.CommandUpdateOrCreate(req_sys_com_db)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.System}/{GlobalStatic.Routes.Commands}/{GlobalStatic.Routes.START}"))
        {
            req_nid = JsonConvert.DeserializeObject<SimpleIdNoiseModel>(payload_json);
            if (req_nid is null)
            {
                _logger.LogError("req_nid is null. error 7E1D1322-FA6D-4DDB-A9E3-1B61269445AE");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req_nid is null. error 7E1D1322-FA6D-4DDB-A9E3-1B61269445AE")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _sys_com_service.CommandRun(req_nid.Id)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.System}/{GlobalStatic.Routes.Commands}/{GlobalStatic.Routes.DELETE}"))
        {
            req_nid = JsonConvert.DeserializeObject<SimpleIdNoiseModel>(payload_json);
            if (req_nid is null)
            {
                _logger.LogError("req_nid is null. error 0E60251C-C3F0-47F7-8962-A7AECDF3D023");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req_nid is null. error 0E60251C-C3F0-47F7-8962-A7AECDF3D023")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _sys_com_service.CommandDelete(req_nid.Id)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }

        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Users}/{GlobalStatic.Routes.GET}"))
        {
            req_nlid = JsonConvert.DeserializeObject<LongIdNoiseModel>(payload_json);
            if (req_nlid is null)
            {
                _logger.LogError("req_nlid is null. error BEA29037-C0F4-41B8-85FE-CA095C423F52");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req_nlid is null. error BEA29037-C0F4-41B8-85FE-CA095C423F52")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _users_service.GetUser(req_nlid.Id)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Users}/{GlobalStatic.Routes.UPDATE}"))
        {
            req_user_upd = JsonConvert.DeserializeObject<UpdateUserModel>(payload_json);
            if (req_user_upd is null)
            {
                _logger.LogError("req_user_upd is null. error 78A0819A-D991-452C-8682-87E79C02E405");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req_user_upd is null. error 78A0819A-D991-452C-8682-87E79C02E405")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _users_service.UpdateUser(req_user_upd)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Users}/{GlobalStatic.Routes.GET}/{GlobalStatic.Routes.LIST}"))
        {
            req_users_list = JsonConvert.DeserializeObject<UserListGetModel>(payload_json);
            if (req_users_list is null)
            {
                _logger.LogError("req_users_list is null. error 3B1F2351-AB5D-4CC0-BF9B-7AF0BDCB3A9A");
                await PublishMessage(JsonConvert.SerializeObject(ResponseBaseModel.CreateError("req_users_list is null. error 3B1F2351-AB5D-4CC0-BF9B-7AF0BDCB3A9A")), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
                return;
            }
            await PublishMessage(JsonConvert.SerializeObject(await _users_service.UsersGetList(req_users_list)), e.ApplicationMessage.ResponseTopic, _mqtt_settings.Secret, salt, _applicationLifetime.ApplicationStopped);
        }

        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.SHOT}"))
        {

        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.Cameras}"))
        {

        }
        else if (e.ApplicationMessage.Topic.Equals($"{_mqtt_conf.PrefixMqtt}{GlobalStatic.Routes.AB_LOG_SYSTEM}"))
        {

        }
    }

    /// <inheritdoc/>
    public async Task PublishMessage(string body_string, string topic, string? secret, string salt, CancellationToken cancellation_token)
    {
        byte[] payload_bytes = await CipherService.EncryptAsync(body_string, secret ?? CipherService.DefaultSecret, salt);
        MqttPublishMessageModel pub_msg = new(payload_bytes, [topic])
        {
            CorrelationData = Encoding.UTF8.GetBytes(salt),
        };

        if (!_mqttClient.IsConnected)
            await _mqttClient.ConnectAsync(MqttClientOptions);

        MqttPublishMessageResultModel pub_res = await PublishMessage(pub_msg, cancellation_token);

        if (pub_res?.IsSuccess != true)
        {
            _logger.LogError($"!pub_red.IsSuccess ({pub_res?.Message}). error FAB191D8-4648-4705-AEFF-DF0AB762E527");
        }
    }
}