// --------------------------------------------------------------------------------------------------------------------
// <copyright file="Form1.cs" company="Hämmer Electronics">
//   Copyright (c) 2020 All rights reserved.
// </copyright>
// <summary>
//   The main form.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace MQTTnet.GetLockApp.WinForm
{
    using System;
    using System.Configuration;
    using System.Data.SqlClient;
    using System.Dynamic;
    using System.IO;
    using System.Reflection;
    using System.Text;
    using System.Threading.Tasks;
    using System.Timers;
    using System.Windows.Forms;
    using MQTTnet.BlueMeteringApp;
    using MQTTnet.Client.Connecting;
    using MQTTnet.Client.Disconnecting;
    using MQTTnet.Client.Options;
    using MQTTnet.Client.Receiving;
    using MQTTnet.Extensions.ManagedClient;
    using MQTTnet.Formatter;
    using MQTTnet.Protocol;
    using MQTTnet.Server;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using Timer = System.Timers.Timer;

    /// <summary>
    /// The main form.
    /// </summary>
    public partial class Form1 : Form
    {
        /// <summary>
        /// The managed publisher client.
        /// </summary>
        private IManagedMqttClient managedMqttClientPublisher;

        /// <summary>
        /// The managed subscriber client.
        /// </summary>
        private IManagedMqttClient managedMqttClientSubscriber;

        /// <summary>
        /// The MQTT server.
        /// </summary>
        private IMqttServer mqttServer;

        /// <summary>
        /// The port.
        /// </summary>
        private string port = "1883";

        /// <summary>
        /// Initializes a new instance of the <see cref="Form1"/> class.
        /// </summary>
        public Form1()
        {
            this.InitializeComponent();

            var timer = new Timer
            {
                AutoReset = true, Enabled = true, Interval = 1000
            };

            timer.Elapsed += this.TimerElapsed;
        }

        /// <summary>
        /// Handles the publisher connected event.
        /// </summary>
        /// <param name="x">The MQTT client connected event args.</param>
        private static void OnPublisherConnected(MqttClientConnectedEventArgs x)
        {
            // MessageBox.Show($"Publisher Connected to {ConfigurationManager.AppSettings["mqttServer"]}", "ConnectHandler", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        /// <summary>
        /// Handles the publisher disconnected event.
        /// </summary>
        /// <param name="x">The MQTT client disconnected event args.</param>
        private static void OnPublisherDisconnected(MqttClientDisconnectedEventArgs x)
        {
            // MessageBox.Show("Publisher Disconnected", "ConnectHandler", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        /// <summary>
        /// Handles the subscriber connected event.
        /// </summary>
        /// <param name="x">The MQTT client connected event args.</param>
        private static void OnSubscriberConnected(MqttClientConnectedEventArgs x)
        {
             // MessageBox.Show($"Subscriber Connected to {ConfigurationManager.AppSettings["mqttServer"]}", "ConnectHandler", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        /// <summary>
        /// Handles the subscriber disconnected event.
        /// </summary>
        /// <param name="x">The MQTT client disconnected event args.</param>
        private static void OnSubscriberDisconnected(MqttClientDisconnectedEventArgs x)
        {
            // MessageBox.Show("Subscriber Disconnected", "ConnectHandler", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        /// <summary>
        /// The method that handles the button click to generate a message.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event args.</param>
        private void ButtonGeneratePublishedMessageClick(object sender, EventArgs e)
        {
            var message = $"{{\"dt\":\"{DateTime.Now.ToLongDateString()} {DateTime.Now.ToLongTimeString()}\"}}";
            this.TextBoxPublish.Text = message;
        }

        /// <summary>
        /// The method that handles the button click to publish a message.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event args.</param>
        private async void ButtonPublishClick(object sender, EventArgs e)
        {
            ((Button)sender).Enabled = false;

            try
            {
                var payload = Encoding.UTF8.GetBytes(this.TextBoxPublish.Text);
                var message = new MqttApplicationMessageBuilder().WithTopic(this.TextBoxTopicPublished.Text.Trim()).WithPayload(payload).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce).WithRetainFlag().Build();

                if (this.managedMqttClientPublisher != null)
                {
                    await this.managedMqttClientPublisher.PublishAsync(message);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message, "Error Occurs", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }

            ((Button)sender).Enabled = true;
        }

        /// <summary>
        /// The method that handles the button click to start the publisher.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event args.</param>
        private async void ButtonPublisherStartClick(object sender, EventArgs e)
        {
            var mqttFactory = new MqttFactory();

            var tlsOptions = new MqttClientTlsOptions
            {
                UseTls = false, IgnoreCertificateChainErrors = true, IgnoreCertificateRevocationErrors = true, AllowUntrustedCertificates = true
            };

            var options = new MqttClientOptions
            {
                ClientId = $"ClientPublisherGetLock{Guid.NewGuid()}",
                ProtocolVersion = MqttProtocolVersion.V311,
                ChannelOptions = new MqttClientTcpOptions
                {
                    Server = ConfigurationManager.AppSettings["mqttServer"], Port = int.Parse(ConfigurationManager.AppSettings["mqttServerPort"]), TlsOptions = tlsOptions
                }
            };

            if (options.ChannelOptions == null)
            {
                throw new InvalidOperationException();
            }

            options.Credentials = new MqttClientCredentials
            {
                Username = ConfigurationManager.AppSettings["mqttServerUsername"], Password = Encoding.UTF8.GetBytes(ConfigurationManager.AppSettings["mqttServerPassword"])
            };

            options.CleanSession = true;
            options.KeepAlivePeriod = TimeSpan.FromSeconds(5);
            this.managedMqttClientPublisher = mqttFactory.CreateManagedMqttClient();
            this.managedMqttClientPublisher.UseApplicationMessageReceivedHandler(this.HandleReceivedApplicationMessage);
            this.managedMqttClientPublisher.ConnectedHandler = new MqttClientConnectedHandlerDelegate(OnPublisherConnected);
            this.managedMqttClientPublisher.DisconnectedHandler = new MqttClientDisconnectedHandlerDelegate(OnPublisherDisconnected);

            await this.managedMqttClientPublisher.StartAsync(
                new ManagedMqttClientOptions
                {
                    ClientOptions = options
                });
        }

        /// <summary>
        /// The method that handles the button click to stop the publisher.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event args.</param>
        private async void ButtonPublisherStopClick(object sender, EventArgs e)
        {
            if (this.managedMqttClientPublisher == null)
            {
                return;
            }

            await this.managedMqttClientPublisher.StopAsync();
            this.managedMqttClientPublisher = null;
        }

        /// <summary>
        /// The method that handles the button click to start the server.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event args.</param>
        private async void ButtonServerStartClick(object sender, EventArgs e)
        {
            if (this.mqttServer != null)
            {
                return;
            }

            var storage = new JsonServerStorage();
            storage.Clear();
            this.mqttServer = new MqttFactory().CreateMqttServer();
            var options = new MqttServerOptions();
            options.DefaultEndpointOptions.Port = int.Parse(ConfigurationManager.AppSettings["mqttServerPort"]);
            options.Storage = storage;
            options.EnablePersistentSessions = true;
            options.ConnectionValidator = new MqttServerConnectionValidatorDelegate(
                c =>
                {
                    if (c.ClientId.Length < 10)
                    {
                        c.ReasonCode = MqttConnectReasonCode.ClientIdentifierNotValid;
                        return;
                    }

                    if (c.Username != "username")
                    {
                        c.ReasonCode = MqttConnectReasonCode.BadUserNameOrPassword;
                        return;
                    }

                    if (c.Password != "password")
                    {
                        c.ReasonCode = MqttConnectReasonCode.BadUserNameOrPassword;
                        return;
                    }

                    c.ReasonCode = MqttConnectReasonCode.Success;
                });

            try
            {
                await this.mqttServer.StartAsync(options);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message, "Error Occurs", MessageBoxButtons.OK, MessageBoxIcon.Error);
                await this.mqttServer.StopAsync();
                this.mqttServer = null;
            }
        }

        /// <summary>
        /// The method that handles the button click to stop the server.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event args.</param>
        private async void ButtonServerStopClick(object sender, EventArgs e)
        {
            if (this.mqttServer == null)
            {
                return;
            }

            await this.mqttServer.StopAsync();
            this.mqttServer = null;
        }

        /// <summary>
        /// The method that handles the button click to subscribe to a certain topic.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event args.</param>
        private async void ButtonSubscribeClick(object sender, EventArgs e)
        {
            var topicFilter = new MqttTopicFilter { Topic = "BMETERING/#" };
            await this.managedMqttClientSubscriber.SubscribeAsync(topicFilter);
            
            // MessageBox.Show("Topic /# is subscribed", "Notice", MessageBoxButtons.OK, MessageBoxIcon.Information);
            
            lblSubscribed.Text = "Subscribed to topic /#";
            lblSubscribed.Visible = true;
        }

        /// <summary>
        /// The method that handles the button click to start the subscriber.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event args.</param>
        private async void ButtonSubscriberStartClick(object sender, EventArgs e)
        {
            var mqttFactory = new MqttFactory();

            var tlsOptions = new MqttClientTlsOptions
            {
                UseTls = false, IgnoreCertificateChainErrors = true, IgnoreCertificateRevocationErrors = true, AllowUntrustedCertificates = true
            };

            var options = new MqttClientOptions
            {
                ClientId = $"ClientSubscriberGetLock{Guid.NewGuid()}",
                ProtocolVersion = MqttProtocolVersion.V311,
                ChannelOptions = new MqttClientTcpOptions
                {
                    Server = ConfigurationManager.AppSettings["mqttServer"], Port = int.Parse(ConfigurationManager.AppSettings["mqttServerPort"]), TlsOptions = tlsOptions
                }
            };

            if (options.ChannelOptions == null)
            {
                throw new InvalidOperationException();
            }

            options.Credentials = new MqttClientCredentials
            {
                Username = ConfigurationManager.AppSettings["mqttServerUsername"],
                Password = Encoding.UTF8.GetBytes(ConfigurationManager.AppSettings["mqttServerPassword"])
            };

            options.CleanSession = true;
            options.KeepAlivePeriod = TimeSpan.FromSeconds(5);

            this.managedMqttClientSubscriber = mqttFactory.CreateManagedMqttClient();
            this.managedMqttClientSubscriber.ConnectedHandler = new MqttClientConnectedHandlerDelegate(OnSubscriberConnected);
            this.managedMqttClientSubscriber.DisconnectedHandler = new MqttClientDisconnectedHandlerDelegate(OnSubscriberDisconnected);
            this.managedMqttClientSubscriber.ApplicationMessageReceivedHandler = new MqttApplicationMessageReceivedHandlerDelegate(this.OnSubscriberMessageReceived);

            await this.managedMqttClientSubscriber.StartAsync(
                new ManagedMqttClientOptions
                {
                    ClientOptions = options
                });
        }

        /// <summary>
        /// The method that handles the button click to stop the subscriber.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event args.</param>
        private async void ButtonSubscriberStopClick(object sender, EventArgs e)
        {
            if (this.managedMqttClientSubscriber == null)
            {
                return;
            }

            await this.managedMqttClientSubscriber.StopAsync();
            this.managedMqttClientSubscriber = null;
        }

        /// <summary>
        /// Handles the received application message event.
        /// </summary>
        /// <param name="x">The MQTT application message received event args.</param>
        private void HandleReceivedApplicationMessage(MqttApplicationMessageReceivedEventArgs x)
        {
            var item = $"Timestamp: {DateTime.Now:O} | Topic: {x.ApplicationMessage.Topic} | Payload: {x.ApplicationMessage.ConvertPayloadToString()} | QoS: {x.ApplicationMessage.QualityOfServiceLevel}";
            this.BeginInvoke((MethodInvoker)delegate { this.TextBoxSubscriber.Text = item + Environment.NewLine + this.TextBoxSubscriber.Text; });
        }

       /// <summary>
        /// Handles the received subscriber message event.
        /// </summary>
        /// <param name="x">The MQTT application message received event args.</param>
        private void OnSubscriberMessageReceived(MqttApplicationMessageReceivedEventArgs x)
        {
            var now = DateTime.Now;

            var dateTimeNow = $"{now:O}";

            var item = $"Timestamp: {dateTimeNow} | Topic: {x.ApplicationMessage.Topic} | Payload: {x.ApplicationMessage.ConvertPayloadToString()}";

            var topicPrefix = x.ApplicationMessage.Topic.Split("/")[0];

            if (topicPrefix == "BMETERING" && x.ApplicationMessage.Topic == "BMETERING/UP")
            {
                string idCofre = "";

                string[] splicedTopic = x.ApplicationMessage.Topic.Split('/', StringSplitOptions.None);

                if (splicedTopic.Length > 1)
                {
                    idCofre = splicedTopic[1];
                }

                dynamic payload = JsonConvert.DeserializeObject<dynamic>(x.ApplicationMessage.ConvertPayloadToString());

                long paramsRxTime = payload.@params.rx_time;
                string paramsPort = payload.@params.port;
                bool paramsDuplicate = payload.@params.duplicate;
                long paramsRadioGpsTime = payload.@params.radio.gps_time;
                double paramsRadioDelay = payload.@params.radio.delay;
                int paramsRadioDatarate = payload.@params.radio.datarate;
                long paramsRadioModulationBandwidth = payload.@params.radio.modulation.bandwidth;
                string paramsRadioModulationType = payload.@params.radio.modulation.type;
                int paramsRadioModulationSpreading = payload.@params.radio.modulation.spreading;
                string paramsRadioModulationCoderate = payload.@params.radio.modulation.coderate;
                int paramsRadioHardwareStatus = payload.@params.radio.hardware.status;
                int paramsRadioHardwareChain = payload.@params.radio.hardware.chain;
                long paramsRadioHardwareTmst = payload.@params.radio.hardware.tmst;
                double paramsRadioHardwareSnr = payload.@params.radio.hardware.snr;
                int paramsRadioHardwareRssi = payload.@params.radio.hardware.rssi;
                int paramsRadioHardwareChannel = payload.@params.radio.hardware.channel;
                double paramsRadioHardwareGpsLat = payload.@params.radio.hardware.gps.lat;
                double paramsRadioHardwareGpsLng = payload.@params.radio.hardware.gps.lng;
                int paramsRadioHardwareGpsAlt = payload.@params.radio.hardware.gps.alt;
                double paramsRadioTime = payload.@params.radio.time;
                double paramsRadioFreq = payload.@params.radio.freq;
                int paramsRadioSize = payload.@params.radio.size;
                int paramsCounterUp = payload.@params.counter_up;
                bool paramsLoraHeaderClassB = payload.@params.lora.header.class_b;
                bool paramsLoraHeaderConfirmed = payload.@params.lora.header.confirmed;
                bool paramsLoraHeaderAdr = payload.@params.lora.header.adr;
                bool paramsLoraHeaderAck = payload.@params.lora.header.ack;
                bool paramsLoraHeaderAdrAckReq = payload.@params.lora.header.adr_ack_req;
                int paramsLoraHeaderVersion = payload.@params.lora.header.version;
                int paramsLoraHeaderType = payload.@params.lora.header.type;
                string paramsPayload = payload.@params.payload;
                string paramsEncryptedPayload = payload.@params.encrypted_payload;

                string metaNetwork = payload.meta.network;
                string metaPacketHash = payload.meta.packet_hash;
                string metaApplication = payload.meta.application;
                string metaDeviceAddr = payload.meta.device_addr;
                double metaTime = payload.meta.time;
                string metaDevice = payload.meta.device;
                string metaPacketId = payload.meta.packet_id;
                string metaGateway = payload.meta.gateway;

                string type = payload.@type;

                Nullable<DateTime> paramsRxTimeDateTime = null;
                Nullable<DateTime> paramsRadioGpsTimeDateTime = null;
                Nullable<DateTime> paramsRadioHardwareTmstDateTime = null;
                Nullable<DateTime> paramsRadioTimeDateTime = null;
                Nullable<DateTime> metaTimeDateTime = null;

                if (paramsRxTime != null)
                {
                    paramsRxTimeDateTime = UnixTimeStampToDateTime(Convert.ToInt64(paramsRxTime));
                }

                if (paramsRadioGpsTime != null)
                {
                    paramsRadioGpsTimeDateTime = UnixTimeStampToDateTime(Convert.ToInt64(paramsRadioGpsTime / 1000));
                }

                if (paramsRadioHardwareTmst != null)
                {
                    paramsRadioHardwareTmstDateTime = UnixTimeStampToDateTime(Convert.ToInt64(paramsRadioHardwareTmst));
                }

                if (paramsRadioTime != null)
                {
                    paramsRadioTimeDateTime = UnixTimeStampToDateTime(Convert.ToInt64(paramsRadioTime));
                }

                if (metaTime != null)
                {
                    metaTimeDateTime = UnixTimeStampToDateTime(Convert.ToInt64(metaTime));
                }

                byte[] data = Convert.FromBase64String(paramsPayload);
                string decodedPayloadString = Encoding.UTF8.GetString(data);

                string decodedPayloadStringAux = decodedPayloadString.Replace("[", "").Replace("]", "");
                string[] decodedPayloadArray = decodedPayloadStringAux.Split(",");

                string payloadId = decodedPayloadArray.Length >= 1 ? decodedPayloadArray[0] : null;

                long payloadVolLitersAux;
                long.TryParse(decodedPayloadArray.Length >= 2 ? decodedPayloadArray[1] : "0", out payloadVolLitersAux);
                long? payloadVolLiters = decodedPayloadArray.Length >= 2 ? payloadVolLitersAux : null;

                long payloadTempAux;
                long.TryParse(decodedPayloadArray.Length >= 3 ? decodedPayloadArray[2] : "0", out payloadTempAux);
                long? payloadTemp = decodedPayloadArray.Length >= 3 ? payloadTempAux : null;

                long payloadBattAux;
                long.TryParse(decodedPayloadArray.Length >= 4 ? decodedPayloadArray[3] : "0", out payloadBattAux);
                long? payloadBatt = decodedPayloadArray.Length >= 4 ? payloadBattAux : null;

                long payloadAlarmAux;
                long.TryParse(decodedPayloadArray.Length >= 5 ? decodedPayloadArray[4] : "0", out payloadAlarmAux);
                long? payloadAlarm = decodedPayloadArray.Length >= 5 ? payloadAlarmAux : null;


                SqlConnection conn = new SqlConnection(@$"Server={ConfigurationManager.AppSettings["sqlServer"]};Database={ConfigurationManager.AppSettings["sqlServerDatabase"]};User Id={ConfigurationManager.AppSettings["sqlServerUser"]};Password={ConfigurationManager.AppSettings["sqlServerPassword"]};");
                conn.Open();

                //string insert_query = "INSERT INTO message (id_cofre, info_id, info_ip, info_mac, info_json, data_hash, data_tmst_begin, data_tmst_begin_datetime, data_tmst_end, data_tmst_end_datetime, data_user, data_type, data_currency_total, data_currency_bill_2, data_currency_bill_5, data_currency_bill_10, data_currency_bill_20, data_currency_bill_50, data_currency_bill_100, data_currency_bill_200, data_currency_bill_rejected, data_currency_envelope, data_currency_envelope_total, cod_loja, data_currency_bill, data_currency_bill_total, data_sensor) VALUES (@idCofre, @infoId, @infoIp, @infoMac, @infoJson, @dataHash, @dataTmstBegin, @dataTmstBeginDateTime, @dataTmstEnd, @dataTmstEndDateTime, @dataUser, @dataType, @dataCurrencyTotal, @dataCurrencyB2, @dataCurrencyB5, @dataCurrencyB10, @dataCurrencyB20, @dataCurrencyB50, @dataCurrencyB100, @dataCurrencyB200, @dataCurrencyBREJ, @dataCurrencyEnvelope, @dataCurrencyEnvelopeTotal, @codLoja, @dataCurrencyBill, @dataCurrencyBillTotal, @dataSensor)";
                string insert_query = "INSERT INTO BlueMeteringMessage (BlueMeteringMessageId, ParamsRxTime, ParamsPort, ParamsDuplicate, ParamsRadioGpsTime, ParamsRadioDelay, ParamsRadioDataRate, ParamsRadioModulationBandwidth, ParamsRadioModulationType, ParamsRadioModulationSpreading, ParamsRadioModulationCodeRate, ParamsRadioHardwareStatus, ParamsRadioHardwareChain, ParamsRadioHardwareTmst, ParamsRadioHardwareSnr, ParamsRadioHardwareRssi, ParamsRadioHardwareChannel, ParamsRadioHardwareGpsLat, ParamsRadioHardwareGpsLng, ParamsRadioHardwareGpsAlt, ParamsRadioTime, ParamsRadioFrequency, ParamsRadioSize, ParamsCounterUp, ParamsLoraHeaderClassB, ParamsLoraHeaderConfirmed, ParamsLoraHeaderAdr, ParamsLoraHeaderAck, ParamsLoraHeaderAdrAckReq, ParamsLoraHeaderVersion, ParamsLoraHeaderType, ParamsPayload, ParamsEncryptedPayload, MetaNetwork, MetaPacketHash, MetaApplication, MetaDeviceAddr, MetaTime, MetaDevice, MetaPacketId, MetaGateway, Type, ParamsRxTimeDateTime, ParamsRadioGpsTimeDateTime, ParamsRadioHardwareTmstDateTime, ParamsRadioTimeDateTime, MetaTimeDateTime, PayloadId, PayloadVolLiters, PayloadTemp, PayloadBatt, PayloadAlarm) VALUES (@BlueMeteringMessageId, @ParamsRxTime, @ParamsPort, @ParamsDuplicate, @ParamsRadioGpsTime, @ParamsRadioDelay, @ParamsRadioDataRate, @ParamsRadioModulationBandwidth, @ParamsRadioModulationType, @ParamsRadioModulationSpreading, @ParamsRadioModulationCodeRate, @ParamsRadioHardwareStatus, @ParamsRadioHardwareChain, @ParamsRadioHardwareTmst, @ParamsRadioHardwareSnr, @ParamsRadioHardwareRssi, @ParamsRadioHardwareChannel, @ParamsRadioHardwareGpsLat, @ParamsRadioHardwareGpsLng, @ParamsRadioHardwareGpsAlt, @ParamsRadioTime, @ParamsRadioFrequency, @ParamsRadioSize, @ParamsCounterUp, @ParamsLoraHeaderClassB, @ParamsLoraHeaderConfirmed, @ParamsLoraHeaderAdr, @ParamsLoraHeaderAck, @ParamsLoraHeaderAdrAckReq, @ParamsLoraHeaderVersion, @ParamsLoraHeaderType, @ParamsPayload, @ParamsEncryptedPayload, @MetaNetwork, @MetaPacketHash, @MetaApplication, @MetaDeviceAddr, @MetaTime, @MetaDevice, @MetaPacketId, @MetaGateway, @Type, @ParamsRxTimeDateTime, @ParamsRadioGpsTimeDateTime, @ParamsRadioHardwareTmstDateTime, @ParamsRadioTimeDateTime, @MetaTimeDateTime, @PayloadId, @PayloadVolLiters, @PayloadTemp, @PayloadBatt, @PayloadAlarm)";
                SqlCommand cmd = new SqlCommand(insert_query, conn);

                cmd.Parameters.AddWithValue("@BlueMeteringMessageId", Guid.NewGuid());
                cmd.Parameters.AddWithValue("@ParamsRxTime", paramsRxTime == null ? DBNull.Value : paramsRxTime);
                cmd.Parameters.AddWithValue("@ParamsPort", paramsPort == null ? DBNull.Value : paramsPort);
                cmd.Parameters.AddWithValue("@ParamsDuplicate", paramsDuplicate == null ? DBNull.Value : paramsDuplicate);
                cmd.Parameters.AddWithValue("@ParamsRadioGpsTime", paramsRadioGpsTime == null ? DBNull.Value : paramsRadioGpsTime);
                cmd.Parameters.AddWithValue("@ParamsRadioDelay", paramsRadioDelay == null ? DBNull.Value : paramsRadioDelay);
                cmd.Parameters.AddWithValue("@ParamsRadioDataRate", paramsRadioDatarate == null ? DBNull.Value : paramsRadioDatarate);
                cmd.Parameters.AddWithValue("@ParamsRadioModulationBandwidth", paramsRadioModulationBandwidth == null ? DBNull.Value : paramsRadioModulationBandwidth);
                cmd.Parameters.AddWithValue("@ParamsRadioModulationType", paramsRadioModulationType == null ? DBNull.Value : paramsRadioModulationType);
                cmd.Parameters.AddWithValue("@ParamsRadioModulationSpreading", paramsRadioModulationSpreading == null ? DBNull.Value : paramsRadioModulationSpreading);
                cmd.Parameters.AddWithValue("@ParamsRadioModulationCodeRate", paramsRadioModulationCoderate == null ? DBNull.Value : paramsRadioModulationCoderate);
                cmd.Parameters.AddWithValue("@ParamsRadioHardwareStatus", paramsRadioHardwareStatus == null ? DBNull.Value : paramsRadioHardwareStatus);
                cmd.Parameters.AddWithValue("@ParamsRadioHardwareChain", paramsRadioHardwareChain == null ? DBNull.Value : paramsRadioHardwareChain);
                cmd.Parameters.AddWithValue("@ParamsRadioHardwareTmst", paramsRadioHardwareTmst == null ? DBNull.Value : paramsRadioHardwareTmst);
                cmd.Parameters.AddWithValue("@ParamsRadioHardwareSnr", paramsRadioHardwareSnr == null ? DBNull.Value : paramsRadioHardwareSnr);
                cmd.Parameters.AddWithValue("@ParamsRadioHardwareRssi", paramsRadioHardwareRssi == null ? DBNull.Value : paramsRadioHardwareRssi);
                cmd.Parameters.AddWithValue("@ParamsRadioHardwareChannel", paramsRadioHardwareChannel == null ? DBNull.Value : paramsRadioHardwareChannel);
                cmd.Parameters.AddWithValue("@ParamsRadioHardwareGpsLat", paramsRadioHardwareGpsLat == null ? DBNull.Value : paramsRadioHardwareGpsLat);
                cmd.Parameters.AddWithValue("@ParamsRadioHardwareGpsLng", paramsRadioHardwareGpsLng == null ? DBNull.Value : paramsRadioHardwareGpsLng);
                cmd.Parameters.AddWithValue("@ParamsRadioHardwareGpsAlt", paramsRadioHardwareGpsAlt == null ? DBNull.Value : paramsRadioHardwareGpsAlt);
                cmd.Parameters.AddWithValue("@ParamsRadioTime", paramsRadioTime == null ? DBNull.Value : paramsRadioTime);
                cmd.Parameters.AddWithValue("@ParamsRadioFrequency", paramsRadioFreq == null ? DBNull.Value : paramsRadioFreq);
                cmd.Parameters.AddWithValue("@ParamsRadioSize", paramsRadioSize == null ? DBNull.Value : paramsRadioSize);
                cmd.Parameters.AddWithValue("@ParamsCounterUp", paramsCounterUp == null ? DBNull.Value : paramsCounterUp);
                cmd.Parameters.AddWithValue("@ParamsLoraHeaderClassB", paramsLoraHeaderClassB == null ? DBNull.Value : paramsLoraHeaderClassB);
                cmd.Parameters.AddWithValue("@ParamsLoraHeaderConfirmed", paramsLoraHeaderConfirmed == null ? DBNull.Value : paramsLoraHeaderConfirmed);
                cmd.Parameters.AddWithValue("@ParamsLoraHeaderAdr", paramsLoraHeaderAdr == null ? DBNull.Value : paramsLoraHeaderAdr);
                cmd.Parameters.AddWithValue("@ParamsLoraHeaderAck", paramsLoraHeaderAck == null ? DBNull.Value : paramsLoraHeaderAck);
                cmd.Parameters.AddWithValue("@ParamsLoraHeaderAdrAckReq", paramsLoraHeaderAdrAckReq == null ? DBNull.Value : paramsLoraHeaderAdrAckReq);
                cmd.Parameters.AddWithValue("@ParamsLoraHeaderVersion", paramsLoraHeaderVersion == null ? DBNull.Value : paramsLoraHeaderVersion);
                cmd.Parameters.AddWithValue("@ParamsLoraHeaderType", paramsLoraHeaderType == null ? DBNull.Value : paramsLoraHeaderType);
                cmd.Parameters.AddWithValue("@ParamsPayload", paramsPayload == null ? DBNull.Value : paramsPayload);
                cmd.Parameters.AddWithValue("@ParamsEncryptedPayload", paramsEncryptedPayload == null ? DBNull.Value : paramsEncryptedPayload);
                cmd.Parameters.AddWithValue("@MetaNetwork", metaNetwork == null ? DBNull.Value : metaNetwork);
                cmd.Parameters.AddWithValue("@MetaPacketHash", metaPacketHash == null ? DBNull.Value : metaPacketHash);
                cmd.Parameters.AddWithValue("@MetaApplication", metaApplication == null ? DBNull.Value : metaApplication);
                cmd.Parameters.AddWithValue("@MetaDeviceAddr", metaDeviceAddr == null ? DBNull.Value : metaDeviceAddr);
                cmd.Parameters.AddWithValue("@MetaTime", metaTime == null ? DBNull.Value : metaTime);
                cmd.Parameters.AddWithValue("@MetaDevice", metaDevice == null ? DBNull.Value : metaDevice);
                cmd.Parameters.AddWithValue("@MetaPacketId", metaPacketId == null ? DBNull.Value : metaPacketId);
                cmd.Parameters.AddWithValue("@MetaGateway", metaGateway == null ? DBNull.Value : metaGateway);
                cmd.Parameters.AddWithValue("@Type", type == null ? DBNull.Value : type);
                cmd.Parameters.AddWithValue("@ParamsRxTimeDateTime", paramsRxTimeDateTime == null ? DBNull.Value : paramsRxTimeDateTime);
                cmd.Parameters.AddWithValue("@ParamsRadioGpsTimeDateTime", paramsRadioGpsTimeDateTime == null ? DBNull.Value : paramsRadioGpsTimeDateTime);
                cmd.Parameters.AddWithValue("@ParamsRadioHardwareTmstDateTime", paramsRadioHardwareTmstDateTime == null ? DBNull.Value : paramsRadioHardwareTmstDateTime);
                cmd.Parameters.AddWithValue("@ParamsRadioTimeDateTime", paramsRadioTimeDateTime == null ? DBNull.Value : paramsRadioTimeDateTime);
                cmd.Parameters.AddWithValue("@MetaTimeDateTime", metaTimeDateTime == null ? DBNull.Value : metaTimeDateTime);
                cmd.Parameters.AddWithValue("@PayloadId", payloadId == null ? DBNull.Value : payloadId);
                cmd.Parameters.AddWithValue("@PayloadVolLiters", payloadVolLiters == null ? DBNull.Value : payloadVolLiters);
                cmd.Parameters.AddWithValue("@PayloadTemp", payloadTemp == null ? DBNull.Value : payloadTemp);
                cmd.Parameters.AddWithValue("@PayloadBatt", payloadBatt == null ? DBNull.Value : payloadBatt);
                cmd.Parameters.AddWithValue("@PayloadAlarm", payloadAlarm == null ? DBNull.Value : payloadAlarm);

                cmd.ExecuteNonQuery();

                conn.Close();

                this.BeginInvoke((MethodInvoker)delegate { this.TextBoxSubscriber.Text = item; });
            }
            else
            {
                var fileName = topicPrefix + "_" + dateTimeNow.Replace("-", "").Replace(":", "").Replace(".", "") + ".txt";

                var year = now.Year.ToString("0000");
                var month = now.Month.ToString("00");
                var day = now.Day.ToString("00");
                var hour = now.Hour.ToString("00");

                var fullPath = Path.Combine("..", topicPrefix, "fileHandling", "incoming", $"{year}", $"{month}", $"{day}", $"{hour}");
                var fullFileName = Path.Combine("..", topicPrefix, "fileHandling", "incoming", $"{year}", $"{month}", $"{day}", $"{hour}", fileName);

                try
                {
                    var content = "Message;Topic" + Environment.NewLine + x.ApplicationMessage.ConvertPayloadToString() + ";" + x.ApplicationMessage.Topic;
                    DirectoryInfo di = Directory.CreateDirectory(fullPath);
                    File.WriteAllText(fullFileName, content);
                    //lblFileErr.Visible = false;
                }
                catch (Exception ex)
                {
                    this.BeginInvoke((MethodInvoker)delegate { this.lblFileErr.Visible = true; this.lblFileErr.Text = $"Error writing file: {@fullFileName}"; });
                }

                this.BeginInvoke((MethodInvoker)delegate { this.TextBoxSubscriber.Text = item; });


                Console.WriteLine();

                //var pay = payload.AsJEnumerable();

                //    string infoId = payload.PARAMS;
                //    string infoIp = payload.INFO.IP;
                //    string infoMac = payload.INFO.MAC;
                //    string infoJson = payload.INFO.JSON;

                //    string dataHash = payload.DATA.HASH;
                //    string dataTmstBegin = payload.DATA.TMST_BEGIN;
                //    string dataTmstEnd = payload.DATA.TMST_END;
                //    string dataUser = payload.DATA.USER;
                //    string dataType = payload.DATA.TYPE;
                //    string dataCurrencyTotal = payload.DATA.RTOTAL;
                //    string dataCurrencyB2 = payload.DATA.R2;
                //    string dataCurrencyB5 = payload.DATA.R5;
                //    string dataCurrencyB10 = payload.DATA.R10;
                //    string dataCurrencyB20 = payload.DATA.R20;
                //    string dataCurrencyB50 = payload.DATA.R50;
                //    string dataCurrencyB100 = payload.DATA.R100;
                //    string dataCurrencyB200 = payload.DATA.R200;
                //    string dataCurrencyBREJ = payload.DATA.RREJ;
                //    string dataCurrencyEnvelope = payload.DATA.ENV;
                //    string dataCurrencyEnvelopeTotal = payload.DATA.RENV;
                //    string dataCurrencyBill = payload.DATA.TBILL;
                //    string dataCurrencyBillTotal = payload.DATA.RTBILL;
                //    string dataSensor = payload.DATA.SENSOR;
                //    Nullable<DateTime> dataTmstBeginDateTime = null;
                //    Nullable<DateTime> dataTmstEndDateTime = null;

                //    if (dataTmstBegin != null)
                //    {
                //        dataTmstBeginDateTime = UnixTimeStampToDateTime(Convert.ToInt64(dataTmstBegin));
                //    }

                //    if (dataTmstEnd != null)
                //    {
                //        dataTmstEndDateTime = UnixTimeStampToDateTime(Convert.ToInt64(dataTmstEnd));
                //    }

                //    SqlConnection conn = new SqlConnection(@$"Server={ConfigurationManager.AppSettings["sqlServer"]};Database={ConfigurationManager.AppSettings["sqlServerDatabase"]};User Id={ConfigurationManager.AppSettings["sqlServerUser"]};Password={ConfigurationManager.AppSettings["sqlServerPassword"]};");
                //    conn.Open();

                //    SqlCommand command = new SqlCommand("Select id_cofre, info_id, data_hash, trackCreationTime, trackLastWriteTime from message where id_cofre=@idCofre and info_id=@infoId and data_hash=@dataHash", conn);
                //    command.Parameters.AddWithValue("@idCofre", idCofre);
                //    command.Parameters.AddWithValue("@infoId", infoId);
                //    command.Parameters.AddWithValue("@dataHash", dataHash);

                //    using (SqlDataReader reader = command.ExecuteReader())
                //    {
                //        if (!reader.Read())
                //        {
                //            reader.Close();

                //            SqlCommand command2 = new SqlCommand("select cod_loja from cofre where id_cofre=@idCofre", conn);
                //            command2.Parameters.AddWithValue("@idCofre", idCofre);

                //            string codLoja = null;

                //            using (SqlDataReader reader2 = command2.ExecuteReader())
                //            {
                //                if (reader2.Read())
                //                {
                //                    codLoja = reader2["cod_loja"].ToString();
                //                }
                //                reader2.Close();
                //            }

                //            string insert_query = "INSERT INTO message (id_cofre, info_id, info_ip, info_mac, info_json, data_hash, data_tmst_begin, data_tmst_begin_datetime, data_tmst_end, data_tmst_end_datetime, data_user, data_type, data_currency_total, data_currency_bill_2, data_currency_bill_5, data_currency_bill_10, data_currency_bill_20, data_currency_bill_50, data_currency_bill_100, data_currency_bill_200, data_currency_bill_rejected, data_currency_envelope, data_currency_envelope_total, cod_loja, data_currency_bill, data_currency_bill_total, data_sensor) VALUES (@idCofre, @infoId, @infoIp, @infoMac, @infoJson, @dataHash, @dataTmstBegin, @dataTmstBeginDateTime, @dataTmstEnd, @dataTmstEndDateTime, @dataUser, @dataType, @dataCurrencyTotal, @dataCurrencyB2, @dataCurrencyB5, @dataCurrencyB10, @dataCurrencyB20, @dataCurrencyB50, @dataCurrencyB100, @dataCurrencyB200, @dataCurrencyBREJ, @dataCurrencyEnvelope, @dataCurrencyEnvelopeTotal, @codLoja, @dataCurrencyBill, @dataCurrencyBillTotal, @dataSensor)";
                //            SqlCommand cmd = new SqlCommand(insert_query, conn);

                //            cmd.Parameters.AddWithValue("@idCofre", idCofre == null ? DBNull.Value : idCofre);
                //            cmd.Parameters.AddWithValue("@infoId", infoId == null ? DBNull.Value : infoId);
                //            cmd.Parameters.AddWithValue("@infoIp", infoIp == null ? DBNull.Value : infoIp);
                //            cmd.Parameters.AddWithValue("@infoMac", infoMac == null ? DBNull.Value : infoMac);
                //            cmd.Parameters.AddWithValue("@infoJson", infoJson == null ? DBNull.Value : infoJson);

                //            cmd.Parameters.AddWithValue("@dataHash", dataHash == null ? DBNull.Value : dataHash);
                //            cmd.Parameters.AddWithValue("@dataTmstBegin", dataTmstBegin == null ? DBNull.Value : dataTmstBegin);
                //            cmd.Parameters.AddWithValue("@dataTmstBeginDateTime", dataTmstBeginDateTime == null ? DBNull.Value : dataTmstBeginDateTime);
                //            cmd.Parameters.AddWithValue("@dataTmstEnd", dataTmstEnd == null ? DBNull.Value : dataTmstEnd);
                //            cmd.Parameters.AddWithValue("@dataTmstEndDateTime", dataTmstEndDateTime == null ? DBNull.Value : dataTmstEndDateTime);
                //            cmd.Parameters.AddWithValue("@dataUser", dataUser == null ? DBNull.Value : dataUser);
                //            cmd.Parameters.AddWithValue("@dataType", dataType == null ? DBNull.Value : dataType);
                //            cmd.Parameters.AddWithValue("@dataCurrencyTotal", dataCurrencyTotal == null ? DBNull.Value : dataCurrencyTotal);
                //            cmd.Parameters.AddWithValue("@dataCurrencyB2", dataCurrencyB2 == null ? DBNull.Value : dataCurrencyB2);
                //            cmd.Parameters.AddWithValue("@dataCurrencyB5", dataCurrencyB5 == null ? DBNull.Value : dataCurrencyB5);
                //            cmd.Parameters.AddWithValue("@dataCurrencyB10", dataCurrencyB10 == null ? DBNull.Value : dataCurrencyB10);
                //            cmd.Parameters.AddWithValue("@dataCurrencyB20", dataCurrencyB20 == null ? DBNull.Value : dataCurrencyB20);
                //            cmd.Parameters.AddWithValue("@dataCurrencyB50", dataCurrencyB50 == null ? DBNull.Value : dataCurrencyB50);
                //            cmd.Parameters.AddWithValue("@dataCurrencyB100", dataCurrencyB100 == null ? DBNull.Value : dataCurrencyB100);
                //            cmd.Parameters.AddWithValue("@dataCurrencyB200", dataCurrencyB200 == null ? DBNull.Value : dataCurrencyB200);
                //            cmd.Parameters.AddWithValue("@dataCurrencyBREJ", dataCurrencyBREJ == null ? DBNull.Value : dataCurrencyBREJ);
                //            cmd.Parameters.AddWithValue("@dataCurrencyEnvelope", dataCurrencyEnvelope == null ? DBNull.Value : dataCurrencyEnvelope);
                //            cmd.Parameters.AddWithValue("@dataCurrencyEnvelopeTotal", dataCurrencyEnvelopeTotal == null ? DBNull.Value : dataCurrencyEnvelopeTotal);

                //            cmd.Parameters.AddWithValue("@codLoja", codLoja == null ? DBNull.Value : codLoja);

                //            cmd.Parameters.AddWithValue("@dataCurrencyBill", dataCurrencyBill == null ? DBNull.Value : dataCurrencyBill);
                //            cmd.Parameters.AddWithValue("@dataCurrencyBillTotal", dataCurrencyBillTotal == null ? DBNull.Value : dataCurrencyBillTotal);
                //            cmd.Parameters.AddWithValue("@dataSensor", dataSensor == null ? DBNull.Value : dataSensor);

                //            cmd.ExecuteNonQuery();
                //        }
                //    }

                //    conn.Close();

                //    try
                //    {
                //        var ackTopic = $"/{idCofre}/COMMAND";
                //        var ackPayload = $@"{{ ""ACK"": {{ ""HASH"":""{dataHash}"", ""TYPE"":{dataType} }} }}";
                //        var message = new MqttApplicationMessageBuilder().WithTopic(ackTopic).WithPayload(ackPayload).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce).WithRetainFlag(false).Build();

                //        if (this.managedMqttClientPublisher != null)
                //        {
                //            Task.Run(async () => await this.managedMqttClientPublisher.PublishAsync(message));
                //        }
                //    }
                //    catch (Exception ex)
                //    {
                //        MessageBox.Show(ex.Message, "Error Occurs", MessageBoxButtons.OK, MessageBoxIcon.Error);
                //    }
                //}
                //else 
                //{
                //    var fileName = topicPrefix + "_" + dateTimeNow.Replace("-", "").Replace(":", "").Replace(".", "") + ".txt";

                //    var year = now.Year.ToString("0000");
                //    var month = now.Month.ToString("00");
                //    var day = now.Day.ToString("00");
                //    var hour = now.Hour.ToString("00");

                //    var fullPath = Path.Combine("..", topicPrefix, "fileHandling", "incoming", $"{year}", $"{month}", $"{day}", $"{hour}");
                //    var fullFileName = Path.Combine("..", topicPrefix, "fileHandling", "incoming", $"{year}", $"{month}", $"{day}", $"{hour}", fileName);

                //    try
                //    {
                //        var content = "Message;Topic" + Environment.NewLine + x.ApplicationMessage.ConvertPayloadToString() + ";" + x.ApplicationMessage.Topic;
                //        DirectoryInfo di = Directory.CreateDirectory(fullPath);
                //        File.WriteAllText(fullFileName, content);
                //        //lblFileErr.Visible = false;
                //    }
                //    catch (Exception ex)
                //    {
                //        this.BeginInvoke((MethodInvoker)delegate { this.lblFileErr.Visible = true; this.lblFileErr.Text = $"Error writing file: {@fullFileName}"; });
                //    }

                //    this.BeginInvoke((MethodInvoker)delegate { this.TextBoxSubscriber.Text = item; });
            }
        }

        private string getPathFromTopicPrefix(string topicPrefix)
        {
            return ".";
        }

        public static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
        {
            // Unix timestamp is seconds past epoch
            DateTime dateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            dateTime = dateTime.AddSeconds(unixTimeStamp).ToLocalTime();
            return dateTime;
        }
        /// <summary>
        /// The method that handles the text changes in the text box.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event args.</param>
        private void TextBoxPortTextChanged(object sender, EventArgs e)
        {
            // ReSharper disable once StyleCop.SA1126
            if (int.TryParse(this.TextBoxPort.Text, out _))
            {
                this.port = this.TextBoxPort.Text.Trim();
            }
            else
            {
                this.TextBoxPort.Text = this.port;
                this.TextBoxPort.SelectionStart = this.TextBoxPort.Text.Length;
                this.TextBoxPort.SelectionLength = 0;
            }
        }

        /// <summary>
        /// The method that handles the timer events.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event args.</param>
        private void TimerElapsed(object sender, ElapsedEventArgs e)
        {
            this.BeginInvoke(
                (MethodInvoker)delegate
                {
                    // Server
                    this.TextBoxPort.Enabled = this.mqttServer == null;
                    this.ButtonServerStart.Enabled = this.mqttServer == null;
                    this.ButtonServerStop.Enabled = this.mqttServer != null;

                    // Publisher
                    this.ButtonPublisherStart.Enabled = this.managedMqttClientPublisher == null;
                    this.ButtonPublisherStop.Enabled = this.managedMqttClientPublisher != null;

                    // Subscriber
                    this.ButtonSubscriberStart.Enabled = this.managedMqttClientSubscriber == null;
                    this.ButtonSubscriberStop.Enabled = this.managedMqttClientSubscriber != null;
                });
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            ButtonSubscriberStartClick(sender, e);
            ButtonSubscribeClick(sender, e);

            ButtonPublisherStartClick(sender, e);
        }

    }
}
