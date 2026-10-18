using ab.context;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MudBlazor.Services;
using Newtonsoft.Json;
using ServerLib;
using SharedLib;
using System.Reflection;

namespace AbLogNet;

public static class MauiProgram
{
    public static MauiApp CreateMauiApp()
    {
        GlobalStatic.PrefixDbFile = "-client";
        var builder = MauiApp.CreateBuilder();
        builder
            .UseMauiApp<App>()
            .ConfigureFonts(fonts =>
            {
                fonts.AddFont("OpenSans-Regular.ttf", "OpenSansRegular");
            });

        builder.Services.AddMauiBlazorWebView();

#if DEBUG
        builder.Services.AddBlazorWebViewDeveloperTools();
        builder.Logging.AddDebug();
#endif
        builder.Services.AddMudServices();

        Assembly assembly = Assembly.GetExecutingAssembly();
        string[] ManifestResourceNames = assembly.GetManifestResourceNames();
        string resourceName = ManifestResourceNames.First(s => s.EndsWith("conf.json", StringComparison.CurrentCultureIgnoreCase));

        using (var stream = assembly.GetManifestResourceStream(resourceName))
        {
            if (stream == null)
            {
                throw new InvalidOperationException("Could not load manifest resource stream.");
            }
            builder.Configuration.AddJsonStream(stream);
        }


        ClientConfigModel settings = builder.Configuration.Get<ClientConfigModel>() ?? throw new Exception("ClientConfigModel is null. error {465BFF7A-217B-4D82-B64D-CEB485B49520}");
        builder.Services.AddSingleton(settings);
        builder.Services.AddHttpClient<ToolsRemoteService>();

        builder.Services.AddSingleton<IParametersStorageService, ParametersStorageRemoteService>();
        builder.Services.AddSingleton<ISystemCommandsService, SystemCommandsMqttService>();
        builder.Services.AddSingleton<IHardwiresService, HardwiresMqttService>();
        builder.Services.AddSingleton<IMqttBaseService, MqttClientService>();
        builder.Services.AddSingleton<IEmailService, EmailLocalService>();
        builder.Services.AddSingleton<IUsersService, UsersMqttService>();
        builder.Services.AddScoped<IToolsService, ToolsRemoteService>();

        builder.Services.AddScoped<IScriptsService, ScriptsMqttService>();
        builder.Services.AddScoped<ICommandsService, CommandsMqttService>();
        builder.Services.AddScoped<IConditionsService, ConditionsMqttService>();
        builder.Services.AddScoped<IContentionsService, ContentionsMqttService>();
        builder.Services.AddScoped<ITriggersService, TriggersMqttService>();

        builder.Services.AddSingleton<INotifyService, NotifyService>();

        builder.Services.AddDbContextFactory<ParametersContext>(opt =>
           opt.UseSqlite());

        using ParametersContext _context = new();
        string _mqttConfig = _context.GetStoredParameter(nameof(MqttConfigModel), "").StoredValue;
        MqttConfigModel mqtt_settings = JsonConvert.DeserializeObject<MqttConfigModel>(_mqttConfig) ?? new() { Password = "", Secret = "", Server = "", Username = "" };
        builder.Services.AddSingleton(x => mqtt_settings);

        MqttClientFactory mqttFactory = new();
        builder.Services.AddTransient(x => mqttFactory);
        IMqttClient mqttClient = mqttFactory.CreateMqttClient();
        builder.Services.AddSingleton(x => mqttClient);

        MauiApp maui_app = builder.Build();
        if (mqtt_settings.AutoStart && mqtt_settings.IsConfigured)
        {
            IHostApplicationLifetime _applicationLifetime = maui_app.Services.GetRequiredService<IHostApplicationLifetime>();
            IMqttBaseService _mqtt_cli_srv = maui_app.Services.GetRequiredService<IMqttBaseService>();
            Task.Run(async () => { await _mqtt_cli_srv.StartService(_applicationLifetime.ApplicationStopping); });
        }

        return maui_app;
    }
}
