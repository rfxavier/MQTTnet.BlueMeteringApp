using System.Collections.Generic;

namespace MQTTnet.BlueMeteringApp
{
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse);
    public class Gps
    {
        public double lat { get; set; }
        public double lng { get; set; }
        public int alt { get; set; }
    }

    public class Hardware
    {
        public int status { get; set; }
        public int chain { get; set; }
        public int tmst { get; set; }
        public double snr { get; set; }
        public int rssi { get; set; }
        public int channel { get; set; }
        public Gps gps { get; set; }
    }

    public class Header
    {
        public bool class_b { get; set; }
        public bool confirmed { get; set; }
        public bool adr { get; set; }
        public bool ack { get; set; }
        public bool adr_ack_req { get; set; }
        public int version { get; set; }
        public int type { get; set; }
    }

    public class JsonPayload
    {
        public Params @params { get; set; }
        public Meta meta { get; set; }
        public string type { get; set; }
    }

    public class Lora
    {
        public Header header { get; set; }
        public List<object> mac_commands { get; set; }
    }

    public class Meta
    {
        public string network { get; set; }
        public string packet_hash { get; set; }
        public string application { get; set; }
        public string device_addr { get; set; }
        public double time { get; set; }
        public string device { get; set; }
        public string packet_id { get; set; }
        public string gateway { get; set; }
    }

    public class Modulation
    {
        public int bandwidth { get; set; }
        public string type { get; set; }
        public int spreading { get; set; }
        public string coderate { get; set; }
    }

    public class Params
    {
        public double rx_time { get; set; }
        public int port { get; set; }
        public bool duplicate { get; set; }
        public Radio radio { get; set; }
        public int counter_up { get; set; }
        public Lora lora { get; set; }
        public string payload { get; set; }
        public string encrypted_payload { get; set; }
    }

    public class Radio
    {
        public long gps_time { get; set; }
        public double delay { get; set; }
        public int datarate { get; set; }
        public Modulation modulation { get; set; }
        public Hardware hardware { get; set; }
        public double time { get; set; }
        public double freq { get; set; }
        public int size { get; set; }
    }

    public class Root
    {
        public JsonPayload jsonPayload { get; set; }
    }
}
