// Mqtt Bridge from mqtt broker to cloud Hive mqtt broker
// Interface via Serial2
// Based on standard framework.
// Subscribes to Hive with # (all topics)
// Any topics arriving on Serial2 published to Hive
// Record kept of recent topics to hive as blocklist
// Any topics from hive sent to Serial2 unless in blocklist
// This blocks superflous topic echo.

// note: bridge does not set esp time or see dns - but feeds it
// dns Register mesage sent on Serial2
// works on DHCP for IP

//to sort = mqttin / send count - send is zero

const char* module = "mqttBridge-l";
const char* buildDate = __DATE__  "  "  __TIME__;

// ----------- start properties include 1 --------------------
#include <SPIFFS.h>
#include <ArduinoJson.h>
String propFile = "/props.properties";   // prop file name
#define PROPSSIZE 40
String propNames[PROPSSIZE];
int propNamesSz = 0;
#define BUFFLEN 200
char buffer[BUFFLEN];
int bufPtr = 0;
#include <esp_task_wdt.h>
#include <EEPROM.h>
// ----------- end properties include 1 --------------------

// ----------- start WiFi & Mqtt & Telnet include 1 ---------------------
#define HAVEWIFI 1
// sort of a DNS
#define DNSSIZE 20
#define SYNCHINTERVAL 30
// wifi, time, mqtt
#include <ESP32Time.h>
ESP32Time rtc;
#include <WiFi.h>
#include "ESPTelnet.h"
#include "EscapeCodes.h"
#include <PicoMQTT.h>
ESPTelnet telnet;
IPAddress ip;
//PicoMQTT::Client mqttClient;  - use secure one

// ----------- end  WiFi & Mqtt & Telnet include 1 ---------------------

// ---------- start custom ------------------------
#include <WiFiClientSecure.h>
WiFiClientSecure espClient;
PicoMQTT::Client mqttClient(espClient);
// serial2
#define RXD2 16
#define TXD2 17
#define BRATE 115200
// hive certificate
// ISRG Root X1 cert.. to 4/6/2035
const char* ca_cert = \
"-----BEGIN CERTIFICATE-----\n" \
"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\n" \
"-----END CERTIFICATE-----";

// ---------- end custom ------------------------

// --------- standard properties ------------------
int logLevel = 2;
String logLevelN = "logLevel";
int eeWriteLimit = 100;
String eeWriteLimitN = "eeWriteLimit";
String wifiSsid = "<ssid>";
String wifiSsidN = "wifiSsid";        
String wifiPwd = "<pwd>";
String wifiPwdN = "wifiPwd";
byte wifiIp4 = 0;   // > 0 for fixed ip
String wifiIp4N = "wifiIp4";
byte mqttIp4 = 200;
String mqttIp4N = "mqttIp4";
int mqttPort = 1883;
String mqttPortN = "mqttPort";   
int telnetPort = 23;
String telnetPortN = "telnetport";   
String mqttId = "xx";          // is username-N and token xx/n/.. from unitId
String mqttIdN = "mqttId";   
int unitId  = 9;                  // uniquness of mqtt id
String unitIdN = "unitId";
int wdTimeout = 30;
String wdTimeoutN = "wdTimeout";
// generic way of setting property as 2 property setting operations
String propNameA = "";
String propNameN = "propName";
String propValue = "";
String propValueN = "propValue";
// these used to apply adjustment via props system
String restartN = "restart";
String writePropsN = "writeProps";

// ------- custom properties -----------
String mqttServer = "<>";
String mqttServerN = "mqttServer";
String mqttUser = "<>";          
String mqttUserN = "mqttUser";
String mqttPwd = "<>";          
String mqttPwdN = "mqttPwd";
bool insecure = false;
String insecureN = "insecure";
// ------- end custom properties -----------

// ----------- start properties include 2 --------------------

bool mountSpiffs()
{
   if(!SPIFFS.begin(true))
  {
    log(1, "SPIFFS Mount Failed");
    return false;
  }
  else
  {
    log(1, "SPIFFS formatted");
  }
  return true;
}

// checks a property name in json doc and keeps a list in propNames
bool checkProp(JsonDocument &doc, String propName, bool reportMissing)
{
  if (propNamesSz >= PROPSSIZE)
  {
    log(0, "!! props names limit");
  }
  else
  {
    propNames[propNamesSz++] = propName;
  }
  if (doc.containsKey(propName))
  {
    String val = doc[propName].as<String>();
    log(0, propName + "=" + val);
    return true;
  }
  if (reportMissing)
  {
    log(0, propName + " missing");
  }
  return false;
}

bool readProps()
{
  log(0, "Reading file " + propFile);
  File file = SPIFFS.open(propFile);
  if(!file || file.isDirectory())
  {
    log(0, "− failed to open file for reading");
    return false;
  }
  JsonDocument doc;
  DeserializationError error = deserializeJson(doc, file);
  if (error) 
  {
    log(0, "deserializeJson() failed: ");
    log(0, error.f_str());
    return false;
  }
  extractProps(doc, true);
  return true;
}

// writes/displays the props
bool writeProps(bool noWrite)
{
  JsonDocument doc;
  addProps(doc);
  String s;
  serializeJsonPretty(doc, s);
  s = s.substring(3,s.length()-2);
  log(0, s);
  if (noWrite)
  {
    return true;
  }
  log(0, "Writing file:" + propFile);
  File file = SPIFFS.open(propFile, FILE_WRITE);
  if(!file)
  {
    log(0, "− failed to open file for write");
    return false;
  }
  serializeJson(doc, file);
  file.close();
  return true;
}

// is expected to be form 'name=value' or 'name value' and can be a comma sep list
// name can be case insensitve match on first unique characters..
// converted to Json to update
void adjustProp(String s)
{
  String ss = s;
  while (true)
  {
    int p1 = ss.indexOf(',');
    if (p1 < 0)
    {
      adjustProp2(ss);
      return;
    }
    String s1 = ss.substring(0, p1);
    adjustProp2(s1);
    ss = ss.substring(p1+1);
  }
}
void adjustProp2(String s)
{
  int p1 = s.indexOf('=');
  if (p1 < 0)
  {
    p1 = s.indexOf(' ');
  }
  if (p1 < 0)
  {
    log(0, "no = or space found");
    return;
  }
  String p = s.substring(0,p1);
  String pl = p;
  pl.toLowerCase();
  String v = s.substring(p1+1);
  int ip;
  int m = 0;
  for (int ix = 0; ix < propNamesSz; ix++)
  {
    if (propNames[ix].length() >= pl.length())
    {
      String pn = propNames[ix].substring(0, pl.length());
      pn.toLowerCase();
      if (pl == pn)
      {
        if (m == 0)
        {
          ip = ix;
          m++;
        }
        else
        {
          if (m == 1)
          {
            log(0, "duplicate match " + p + " " + propNames[ip]);
          }
          m++;
          log(0, "duplicate match " + p + " " + propNames[ix]);
        }
      }
    }
  }
  if (m > 1)
  {
    return;
  }
  else if (m==0)
  {
    log(0, "no match " + p);
    return;
  }
  s = "{\"" + propNames[ip] + "\":\"" + v + "\"}";
 
  JsonDocument doc;
  DeserializationError error = deserializeJson(doc, s);
  if (error) 
  {
    log(0, "deserializeJson() failed: ");
    log(0, error.f_str());
    return;
  }
  extractProps(doc, false);
}

// logger
void log(int level, String s)
{
  if (level > logLevel) return;
  Serial.println(s);
  #if HAVEWIFI
  telnet.println(s);
  #endif
}
void log(int level)
{
  if (level > logLevel) return;
  Serial.println();
  #if HAVEWIFI
  telnet.println();
  #endif
}
void loga(int level, String s)
{
  if (level > logLevel) return;
  Serial.print(s);
  #if HAVEWIFI
  telnet.print(s);
  #endif
}

void checkSerial()
{
  while (Serial.available())
  {
    char c = Serial.read();
    switch (c)
    {
      case 0:
        break;
      case '\r':
        break;
      case '\n':
        buffer[bufPtr++] = 0;
        processCommandLine(String(buffer));
        bufPtr = 0;
        break;
      default:
        if (bufPtr < BUFFLEN -1)
        {
          buffer[bufPtr++] = c;
        }
        break;
    }
  } 
}
// for counting restarts - write to eprom
struct eeStruct
{
  unsigned int writes = 0;
  unsigned int wdtRestart = 0;
  unsigned int isrRestart = 0;
  unsigned int panicRestart = 0;
  unsigned int otherRestart = 0;
};

eeStruct eeData;

// for reliability stats from each restart
int getGatewayCount = 0;
int getWifiCount = 0;
int reConnWifiCount = 0;
unsigned long mqttDiscMs = 0;
int mqttConnCount = 0;
int mqttDiscCount = 0;
int mqttConnFailCount = 0;
int mqttConnTimeMs = 0;
int mqttSendCount = 0;
int mqttInCount = 0;

void eeDataReset()
{
  eeData.writes = 0;
  eeData.isrRestart = 0;
  eeData.wdtRestart = 0;
  eeData.panicRestart = 0;
  eeData.otherRestart = 0;
}

void eepromInit()
{
  int eeSize = sizeof(eeData);
  EEPROM.begin(eeSize);
  log(0, "ee size "+ String(eeSize));
}
void eepromWrite()
{
  eeData.writes++;
  if (eeData.writes > eeWriteLimit)
  {
    log(0, "eeprop Write limit..");
    return;
  }
  EEPROM.put(0, eeData);
  EEPROM.commit();
  log(0, "eewrite:" + String(eeData.writes));
}
void eepromRead()
{
  EEPROM.get(0, eeData);
  log(0, "eeWrites: " + String(eeData.writes));
}
void checkRestartReason()
{
  eepromRead();
  int resetReason = esp_reset_reason();
  log(0, "ResetReason: " + String(resetReason));
  switch (resetReason)
  {
    case ESP_RST_POWERON:
      return;// ok
    case ESP_RST_PANIC:
      eeData.panicRestart++;
      break;
    case ESP_RST_INT_WDT:
      eeData.isrRestart++;
      break;
    case ESP_RST_TASK_WDT:
      eeData.wdtRestart++;
      break;
    default:
      eeData.otherRestart++;
      break;
  }
  eepromWrite();
  logResetStats();
}

void logResetStats()
{
  log(0, "eeWrites: " + String(eeData.writes));
  log(0, "panic: " + String(eeData.panicRestart));
  log(0, "taskwd: " + String(eeData.wdtRestart));
  log(0, "irswd: " + String(eeData.isrRestart));
  log(0, "other: " + String(eeData.otherRestart));
  #if HAVEWIFI
  log(0, "getGateway: " + String(getGatewayCount));
  log(0, "getWifi: " + String(getWifiCount));
  log(0, "reconnWifi: " + String(reConnWifiCount));
  log(0, "mqttConn: " + String(mqttConnCount));
  log(0, "mqttConnT: " + String(mqttConnTimeMs));
  log(0, "mqttDisc: " + String(mqttDiscCount));
  log(0, "mqttFail: " + String(mqttConnFailCount));
  log(0, "mqttSend: " + String(mqttSendCount));
  log(0, "mqttIn: " + String(mqttInCount));
  log(0, "wfChannel: " + String(WiFi.channel()));
  log(0, "wfRSSI: " + String(WiFi.RSSI()));
  log(0, "wfPower: " + String(WiFi.getTxPower()));
  #endif
}
// ----------- end properties include 2 --------------------

// ----------- custom properties modify section  --------------------
// extract and add properties to json doc
// customize this for props expected and data types - watch with bools

void extractProps(JsonDocument &doc, bool reportMissing)
{
  propNamesSz = 0;
  log(0, "setting properties:");
  String propName;
  propName = logLevelN; if (checkProp(doc, propName, reportMissing)) logLevel = doc[propName].as<int>();
  propName = eeWriteLimitN; if (checkProp(doc, propName, reportMissing)) eeWriteLimit = doc[propName].as<int>();
  propName = wifiSsidN; if (checkProp(doc, propName, reportMissing)) wifiSsid = doc[propName].as<String>();
  propName = wifiPwdN;  if (checkProp(doc, propName, reportMissing)) wifiPwd = doc[propName].as<String>();
  propName = wifiIp4N;  if (checkProp(doc, propName, reportMissing)) wifiIp4 = doc[propName].as<byte>();
  propName = mqttPortN; if (checkProp(doc, propName, reportMissing)) mqttPort = doc[propName].as<int>();
  //propName = mqttIp4N;  if (checkProp(doc, propName, reportMissing)) mqttIp4 = doc[propName].as<byte>();
  propName = telnetPortN;if (checkProp(doc, propName, reportMissing)) telnetPort = doc[propName].as<int>();
  propName = mqttIdN;   if (checkProp(doc, propName, reportMissing)) mqttId = doc[propName].as<String>();
  propName = unitIdN;   if (checkProp(doc, propName, reportMissing)) unitId = doc[propName].as<int>();
  propName = wdTimeoutN;if (checkProp(doc, propName, reportMissing)) wdTimeout = max(doc[propName].as<int>(),30);
  // these just for adjustment
  propName = restartN; if (checkProp(doc, propName, false)) ESP.restart();
  propName = writePropsN; if (checkProp(doc, propName, false)) writeProps(false);
  propName = propNameN; if (checkProp(doc, propName, false)) propNameA = doc[propName].as<String>();
  propName = propValueN;if (checkProp(doc, propName, false)) propValue = doc[propName].as<String>();  // picked up in checkState()

  // ----- start custom extract -----
  propName = mqttServerN;   if (checkProp(doc, propName, reportMissing)) mqttServer = doc[propName].as<String>();
  propName = mqttUserN;   if (checkProp(doc, propName, reportMissing)) mqttUser = doc[propName].as<String>();
  propName = mqttPwdN;   if (checkProp(doc, propName, reportMissing)) mqttPwd = doc[propName].as<String>();
  propName = insecureN;   if (checkProp(doc, propName, reportMissing)) insecure = doc[propName].as<int>();

  // ----- end custom extract -----
}

// adds props for props write - customize
void addProps(JsonDocument &doc)
{
  doc[logLevelN] = logLevel;
  doc[eeWriteLimitN] = eeWriteLimit;
  doc[wifiSsidN] = wifiSsid;
  doc[wifiPwdN] = wifiPwd;
  doc[wifiIp4N] = wifiIp4;
  //doc[mqttIp4N] = mqttIp4;
  doc[telnetPortN] = telnetPort;
  doc[mqttPortN] = mqttPort;
  doc[mqttIdN] = mqttId;
  doc[unitIdN] = unitId;
  doc[wdTimeoutN] = wdTimeout;

  // ----- start custom add -----
  doc[mqttServerN] = mqttServer;
  doc[mqttUserN] = mqttUser;
  doc[mqttPwdN] = mqttPwd;
  doc["insecure"] = insecure;
  // ----- end custom add -----
}

// custom modified section for props control and general commands
void processCommandLine(String cmdLine)
{
  if (cmdLine.length() == 0)
  {
    return;
  }
  
  switch (cmdLine[0])
  {
    case 'h':
    case '?':
      log(0, "v:version, w:writeprops, d:dispprops, l:loadprops p<prop>=<val>: change prop, r:restart");
      log(0, "s:showstats, z:zerostats, n:dns, 0,1,2:loglevel = " + String(logLevel));
      return;
    case 'w':
      writeProps(false);
      return;
    case 'd':
      writeProps(true);
      return;
    case 'l':
      readProps();
      return;
    case 'p':
      adjustProp(cmdLine.substring(1));
      return;
    case 'r':
      ESP.restart();
      return;
    case 'v':
      loga(0, module);
      loga(0, " ");
      log(0, buildDate);
      return;
    case 'z':
      eeDataReset();
      return;
    case 's':
      logResetStats();
      return;
    case 't':
      {
        tm now = rtc.getTimeStruct();
        log(0, "ESP time: " + dateTimeIso(now));
        return;
      }
    case 'n':
      logDns();
      break;
    case '0':
      logLevel = 0;
      log(0, " loglevel=" + String(logLevel));
      return;
    case '1':
      logLevel = 1;
      log(0, " loglevel=" + String(logLevel));
      return;
    case '2':
      logLevel = 2;
      log(0, " loglevel=" + String(logLevel));
      return;

  // ----- start custom cmd -----

  // ----- end custom cmd -----
    default:
      log(0, "????");
      return;
  }
}
// ----------- end custom properties modify section  --------------------

// ------------ start wifi and mqtt include section 2 ---------------
IPAddress localIp;
IPAddress gatewayIp;
IPAddress primaryDNSIp;
String mqttMoniker;

int recoveries = 0;
unsigned long seconds = 0;
unsigned long lastSecondMs = 0;
unsigned long startRetryDelay = 0;
int long retryDelayTime = 10;  // seconds
bool retryDelay = false;


// state engine
#define START 0
#define STARTGETGATEWAY 1
#define WAITGETGATEWAY 2
#define STARTCONNECTWIFI 3
#define WAITCONNECTWIFI 4
#define ALLOK 5

String stateS(int state)
{
  if (state == START) return "start";
  if (state == STARTGETGATEWAY) return "startgetgateway";
  if (state == WAITGETGATEWAY) return "waitgetgateway";
  if (state == STARTCONNECTWIFI) return "startconnectwifi";
  if (state == WAITCONNECTWIFI) return "waitconnectwifi";
  if (state == ALLOK) return "allok";
  return "????";
}
int state = START;

unsigned long startWaitWifi;

bool startGetGateway()
{
  getGatewayCount++;
  WiFi.disconnect();
  delay (500);
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(false);
  log(1, "Start wifi dhcp" + wifiSsid + " " + wifiPwd);
  WiFi.begin(wifiSsid, wifiPwd);
  startWaitWifi = millis();
  return true;
}
bool startWifi()
{
  getWifiCount++;
  WiFi.disconnect();
  WiFi.setAutoReconnect(true);
  delay (500);
  unsigned long startWaitWifi = millis();
  WiFi.mode(WIFI_STA);
  if (wifiIp4 == 0)
  {
    log(1, "Start wifi dhcp: " + wifiSsid + " " + wifiPwd);
  }
  else
  {
    IPAddress subnet(255, 255, 0, 0);
    IPAddress fixedIp = localIp;
    fixedIp[3] = wifiIp4;
    if (!WiFi.config(fixedIp, gatewayIp, subnet, primaryDNSIp, primaryDNSIp)) 
    {
      log(1, "STA Failed to configure");
      return false;
    }
    log(1, "Start wifi fixip: " + wifiSsid + " " + wifiPwd);
  }
  WiFi.begin(wifiSsid, wifiPwd);
  return true;
}

int waitWifi()
{
  // 0=waiting, <0=fail, >0=ok
  unsigned long et = millis() - startWaitWifi;
  if (WiFi.status() == WL_CONNECTED)
  {
    localIp = WiFi.localIP();
    gatewayIp = WiFi.gatewayIP();
    primaryDNSIp = WiFi.dnsIP();
    log(1, "connected, t=" + String(et) + ", local=" + localIp.toString() + " gateway=" + gatewayIp.toString() + " dns=" + primaryDNSIp.toString());
    reConnWifiCount--;
    return 1;
  }
  
  if (et > 30000)
  {
    log(1, "... fail wifi connection timeout");
    return -1;
  }
  return 0;
}


// dns support
struct dnsIsh
{
  bool used = false;
  String name;
  String ip;
  int timeout;
};
dnsIsh dnsList[DNSSIZE];
unsigned long dnsVersion = 0;
unsigned long lastSynchTime = 0;

void logDns()
{
  log(0, "dns v=" + String(dnsVersion));
  for (int ix = 0; ix < DNSSIZE; ix++)
  {
    if (dnsList[ix].used && dnsList[ix].timeout > 0)
    {
      log(0, String(ix) + " " + dnsList[ix].name + " " + dnsList[ix].ip);
    }
  }
}

String dnsGetIp(String name)
{
  for (int ix = 0; ix < DNSSIZE; ix++)
  {
    if (dnsList[ix].used && dnsList[ix].name.startsWith(name))
    {
      return dnsList[ix].ip;
    }
  }
  return "";
}

// ESP32 Time
String formatd2(int i)
{
  if (i < 10)
  {
    return "0" + String(i);
  }
  return String(i);
}
String dateTimeIso(tm d)
{
  return String(d.tm_year+1900)+"-"+formatd2(d.tm_mon+1)+"-"+formatd2(d.tm_mday)+"T"+formatd2(d.tm_hour)+":"+formatd2(d.tm_min)+":"+formatd2(d.tm_sec);
}

// time and dns synch
void sendSynch()
{
  // will get updates if not in synch
  JsonDocument doc;
  doc["r"] = mqttMoniker + "/c/s";    // reply token
  doc["n"] = mqttId + String(unitId);
  doc["i"] = localIp.toString();
  doc["e"] = rtc.getEpoch();
  doc["v"] = dnsVersion;
  //mqttSend("mb/s", doc);
  serial2Send("mb/s", doc);   // different for bridge
}

void synchCheck()
{
  if (seconds - lastSynchTime > SYNCHINTERVAL/2)
  {
    lastSynchTime = seconds;
    sendSynch();
  }
}

void processSynch(JsonDocument &doc)
{
  unsigned long epoch = doc["e"].as<unsigned long>();
  if (epoch > 0)
  {
    rtc.setTime(epoch);
    tm now = rtc.getTimeStruct();
    log(2, "espTimeSet: " + dateTimeIso(now));
  }
  else
  {
    int timeAdjust = doc["t"].as<int>();
    if (timeAdjust != 0)
    {
      rtc.setTime(rtc.getEpoch() + timeAdjust);
      log(2, "espTimeAdjust: " + String(timeAdjust));
    }
  }
  long newDnsVersion = doc["v"].as<long>();
  if (newDnsVersion != 0)
  {
    dnsVersion  = newDnsVersion;
    log(2, "dns version: " + String(dnsVersion));
    for (int ix = 0; ix < DNSSIZE; ix++)
    {
      dnsList[ix].used = false;
    }
    for (int ix = 0; ix < DNSSIZE; ix++)
    {
      if (doc.containsKey("n" + String(ix)))
      {
        dnsList[ix].name = doc["n" + String(ix)].as<String>();
        dnsList[ix].ip = doc["i" + String(ix)].as<String>();
        dnsList[ix].used = true;
        dnsList[ix].timeout = 1;   // for consistency with dnsLog
        log(2, ".. " + dnsList[ix].name + " " + dnsList[ix].ip);
      }
      else
      {
        break;
      }
    }
  }
}



// ------------- mqtt section -----------------

// mqtt 
void setupMqttClient()
{
  // IPAddress fixedIp = localIp;
  // fixedIp[3] = mqttIp4;
  // String server = fixedIp.toString();
  // mqttClient.host = server;
  mqttClient.port = mqttPort;
  mqttMoniker = mqttId + "/" + String(unitId);
  mqttClient.client_id = mqttMoniker;
  String topic = mqttMoniker + "/c/#";
  //mqttClient.subscribe(topic, &mqttMessageHandler);   - need block standard handler
  mqttSubscribeAdd();
  mqttClient.connected_callback = [] {mqttConnHandler();};
  mqttClient.disconnected_callback = [] {mqttDiscHandler();};
  mqttClient.connection_failure_callback = [] {mqttFailHandler();};
  mqttClient.begin();
}

// mqtt handlers
void mqttConnHandler()
{
  log(0, "MQTT connected: " + String(millis() - mqttDiscMs));
  sendSynch();
  mqttConnCount++;
}
void mqttDiscHandler()
{
  log(0, "MQTT disconnected");
  mqttDiscCount++;
  mqttDiscMs = millis();
}
void mqttFailHandler()
{
  log(0, "MQTT CONN FAIL");
  if (WiFi.isConnected())
  {
    mqttConnFailCount++;
  }
  mqttDiscMs = millis();
}
void mqttMessageHandler(const char * topicC, Stream & stream)
{
  mqttInCount++;
  String topic = String(topicC);
  JsonDocument doc;
  DeserializationError error = deserializeJson(doc, stream);
  if (error) 
  {
    log(2, "deserialize fail: " +  String(error.f_str()) + " " + topic);
    return;
  }
  if (logLevel >=2)
  {
    String s;
    serializeJson(doc, s);
    log(2, "in:" + topic + " " + s);
  }
  // ignore for bridge as only from hive
  #if 0
  if (topic.endsWith("/p"))
  {   
    // its a property setting
    adjustProp(doc["p"].as<String>());
  }
  else if (topic.endsWith("/s"))
  {   
    // its a synch response message
    processSynch(doc);
  }
  else
  #endif
  {
    handleIncoming(topic, doc);
  }
}

void mqttSend(String topic, JsonDocument &doc)
{
  if (logLevel >=2)
  {
    String s;
    serializeJson(doc, s);
    log(2, "out: " + topic + " " + s);
  }
  if (WiFi.isConnected() && mqttClient.connected())
  {
    // publish using begin_publish()/send() API
    auto publish = mqttClient.begin_publish(topic, measureJson(doc));
    serializeJson(doc, publish);
    publish.send();
    mqttSendCount++;
  }
}

// ------------ telnet --------------
void setupTelnet(int port) 
{  
  telnet.stop();
  // passing on functions for various telnet events
  telnet.onConnect(onTelnetConnect);
  telnet.onDisconnect(onTelnetDisconnect);
  telnet.onConnectionAttempt(onTelnetConnectionAttempt);
  telnet.onReconnect(onTelnetReconnect);
  telnet.onInputReceived(onTelnetInput);

  if (telnet.begin(port)) 
  {
    log(1, "telnet running");
  } 
  else 
  {
    log(1, "telnet start fail");
  }
}

void onTelnetConnect(String ip) 
{
  Serial.println("telnet connected");
  telnet.println("hello..");
}
void onTelnetDisconnect(String ip) 
{
  Serial.println("telnet disconnected");
}

void onTelnetReconnect(String ip) 
{
  Serial.println("telnet reconnected");
}

void onTelnetConnectionAttempt(String ip) 
{
  Serial.println("another telnet tried to connected - disconnecting");
  telnet.println("Another session trying. disconnecting you..");
  telnet.disconnectClient();
}

void onTelnetInput(String str) 
{
  processCommandLine(str);
}

void setRetryDelay()
{
  startRetryDelay = seconds;
  retryDelay = true;
  log(1, "retry delay....");
  recoveries++;
}
bool lastWifiState = false;
// state engine manager
void checkState()
{
  if (propValue != "")
  {
    adjustProp(propNameA + "=" + propValue);
    propValue = "";
  }  
  unsigned long nowMs = millis();
  while (nowMs - lastSecondMs > 1000)
  {
    seconds++;
    lastSecondMs+= 1000;
  }
  synchCheck();
  bool thisWifiState = WiFi.isConnected();
  if (thisWifiState != lastWifiState)
  {
    if (thisWifiState)
    {
      log(0, "WiFi Connected..");
      reConnWifiCount++;
    }
    else
    {
      log(0, "WiFi Disconnected..");
    }
    lastWifiState = thisWifiState;
  }
 
  if (retryDelay)
  {
    if (seconds - startRetryDelay < (retryDelayTime))
    {
      return; // retry wait
    }
    else
    {
      retryDelay = false;
    }
  }

  int res;
  switch (state)
  {
    case START:
      if (wifiIp4 == 0)
      {
        state = STARTCONNECTWIFI;    // dhcp ip
      }
      else
      {
        state = STARTGETGATEWAY;
      }
      return;
    case STARTGETGATEWAY:
      // only get gateway for fixed ip
      if (!startGetGateway())
      {
        setRetryDelay();
        return;
      }
      state = WAITGETGATEWAY;
      return;
    case WAITGETGATEWAY:
      res = waitWifi();
      if (res == 0)
      {
        return;
      }
      if (res < 0)
      {
        setRetryDelay();
        state = STARTGETGATEWAY;
        return;
      }
    case STARTCONNECTWIFI:
      if (!startWifi())
      {
        setRetryDelay();
        return;
      }
      state = WAITCONNECTWIFI;
      return;
    case WAITCONNECTWIFI:
      // mandatory we get connected once before proceeding
      res = waitWifi();
      if (res == 0)
      {
        return;
      }
      if (res < 0)
      {
        setRetryDelay();
        state = STARTCONNECTWIFI;
        return;
      }
      setupTelnet(telnetPort);
      setupMqttClient();
      state = ALLOK;

      return;

    case ALLOK:
      return;
  }
}
// ------------ end wifi and mqtt include section 2 ---------------

// ------------ start wifi and mqtt custom section 2 ---------------
void mqttSubscribeAdd()
{
  // use standard or custom handler
  // start subscribe add
  // sneak in extra settings for hive
  mqttClient.host = mqttServer;
  mqttClient.port = mqttPort;
  mqttClient.username = mqttUser;
  mqttClient.password = mqttPwd;
  if (insecure)
  {
    espClient.setInsecure();
  }
  else
  {
    espClient.setCACert(ca_cert);
  }

  String topic = "#";   // everything
  mqttClient.subscribe(topic, &mqttMessageHandler2);
  // end subscribe add
}
void handleIncoming(String topic, JsonDocument &doc)
{
  // start custom additional incoming
  
  // end custom additional incoming
}
// ------------ end wifi and mqtt custom section 2 ---------------

// ---- start custom code --------------

void bridgeSetup()
{
  Serial2.begin(BRATE, SERIAL_8N1, RXD2, TXD2);     
}

void serial2Send(String topic, JsonDocument doc)
{
  Serial2.print(topic);
  serializeJson(doc, Serial2);
  Serial2.print(char(0));
  if (logLevel >=2)
  {
    String s;
    serializeJson(doc, s);
    log(2, "to bridge: " + topic + " " + s); 
  }
}

// special message handler
void mqttMessageHandler2(const char * topic, const char * message)
{
  if (blockCheck(String(topic)))
  {
    if (logLevel >=2)
    {
      log(2, "blocked Hive: " + String(topic) + " " + String(message)); 
    }
  }
  else
  {
    mqttInCount++;
    if (logLevel >=2)
    {
      log(2, "from Hive: " + String(topic) + " " + String(message)); 
    }
      //push to Serial2 as is with null terminator 
    Serial2.print(topic);
    Serial2.print(message);
    char c = 0;
    Serial2.print(c);
  }
}

// Serial 2 input handling
bool topicBit = true;
#define TBLEN 128
char tb[TBLEN];    // for topic
int tbPtr = 0;
#define MBLEN 1024
char mb[MBLEN];    // for message
int mbPtr = 0;

void bridgeLoop()
{
  blockJanitor();

  // check serial and publish
  while (Serial2.available())
  {
    char c = Serial2.read();
    if (topicBit)
    {
      if (c=='{')
      {
        tb[tbPtr++] = 0;
        topicBit = false;
        mbPtr = 0;
        tbPtr = 0;
        mb[mbPtr++]= c;
      }
      else
      {
        while (tbPtr >= TBLEN-2)
        {
          tbPtr--;
        }
        tb[tbPtr++] = c;
      }
    }
    else
    {
      if (c==0)
      {
        mb[mbPtr++] = 0;
        topicBit = true;
        mbPtr = 0;
        tbPtr = 0;
        String tbS = String(tb);
        if (tbS == mqttMoniker + "/c/s")
        {
          JsonDocument doc;
          deserializeJson(doc, mb);
          processSynch(doc);
        }
        else
        {
          mqttClient.publish(tb, mb);
          if (!tbS.endsWith("echo"))
          {
            blockAdd(tbS);    // round trip test topic
          }
          mqttSendCount++;
        }
        if (logLevel >=2)
        {
          log(2, "to bridge: " + tbS + " " + String(mb)); 
        }
      }
      else
      {
        while (mbPtr >= MBLEN-2)
        {
          mbPtr--;
        }
        mb[mbPtr++] = c;
      }
    }
  }
}

#define BLOCKSIZE 10
#define BLOCKTIMEOUT 10
#define BLOCKINTERVAL 1000
unsigned long lastBlockTidy = 0;
int blockTimeout[BLOCKSIZE];
String blockTopic[BLOCKSIZE];

// does timeout
void blockJanitor()
{
  if (millis() - lastBlockTidy > BLOCKINTERVAL)
  {
    lastBlockTidy = millis();
    for (int ix = 0; ix < BLOCKSIZE; ix++)
    {
      if (blockTimeout[ix] > 0)
      {
        blockTimeout[ix]--;
      }
    }
  }
}
// true if to block
bool blockCheck(String topic)
{
  for (int ix = 0; ix < BLOCKSIZE; ix++)
  {
    if (blockTimeout[ix] > 0 && topic == blockTopic[ix])
    {
      return true;
    }
  }
  return false;
}
void blockAdd(String topic)
{
  for (int ix = 0; ix < BLOCKSIZE; ix++)
  {
    if (topic == blockTopic[ix])
    {
      blockTimeout[ix] = BLOCKTIMEOUT;   // reactivate existing entry
      return;
    }
  }
  for (int it = 0; it < BLOCKTIMEOUT; it++)
  {
    for (int ix = 0; ix < BLOCKSIZE; ix++)
    {
      if (blockTimeout[ix] == it)
      {
        blockTimeout[ix] = BLOCKTIMEOUT;   // take over shortest timeout
        blockTopic[ix] = topic;
        return;
      }
    }
  }
  // rarely should get here.... 
}

// ---- end custom code --------------

void setup()
{
  loga(1, module);
  loga(1, " ");
  log(1, buildDate);
  Serial.begin(115200);
  checkRestartReason();
  mountSpiffs();
  readProps();
  bridgeSetup();
  esp_task_wdt_config_t config;
  int wdt = max(wdTimeout*1000,2000);
  config.timeout_ms = max(wdTimeout*1000,2000);;
  config.idle_core_mask = 3; //both cores
  config.trigger_panic = true;
  esp_task_wdt_reconfigure(&config);
  esp_task_wdt_add(NULL);
}


void loop()
{
  esp_task_wdt_reset();
  telnet.loop();
  mqttClient.loop();
  checkSerial();
  checkState();
  bridgeLoop();
  

  delay(1);
}
