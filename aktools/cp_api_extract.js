/*

{
    "AK_CERT_SIG_ALGO": "RSA-SHA256",
    "AK_CLIENT_CERT_STATUS_MSG": "StatusMsgNotAvailable",
    "AK_CLIENT_CERT_SUBJECT_DN": "SubjectDNnotAvailable",
    "AK_CLIENT_REQUEST_END_TIME": "107",
    "AK_CLIENT_REQUEST_NUMBER": "1",
    "AK_CLIENT_REQUEST_START_TIME": "1720392476.749",
    "AK_CLIENT_RTT": "144",
    "AK_CLIENT_TRANSFER_TIME": "0",
    "AK_CLIENT_TURNAROUND_TIME": "0",
    "AK_CONNECTED_CLIENT_IP": "67.189.12.144",
    "AK_CURRENT_TIME": "1720392476",
    "AK_DEFAULT_IPV6_IP": "2600:1405:800::6864:a855",
    "AK_EIP_FORWARDER_IP": "",
    "AK_END_USER_MAP_DOMAIN": "",
    "AK_END_USER_MAP_IP1": "",I_
    "AK_END_USER_MAP_IP2": "",
    "AK_END_USER_MAP_LIST": "",
    "AK_GHOST_IP": "23.11.32.3",
    "AK_GHOST_SERVICE_IP": "104.100.168.85",
    "AK_MAP": "dscw34",
    "AK_MAPRULE": "",
    "AK_PARENT_MAP": "api-a6068.akasrg.akamai.com",
    "AK_PROTOCOL_NEGOTIATION": "h2",
    "AK_QUIC_EARLY_DATA": "",
    "AK_QUIC_SUPPORTED_VERSIONS": "1,4278190109,50,46,43",
    "AK_QUIC_SUPPORTED_VERSIONS_HEX": "quic=1;quic=FF00001D;quic=51303530;quic=51303436;quic=51303433",
    "AK_REGION": "44239",
    "AK_SERIAL": "119",
    "AK_TLS_CIPHER_NAME": "TLS_AES_256_GCM_SHA384",
    "AK_TLS_ENCRYPTION_BITS": "256",
    "AK_TLS_PREFERRED_CIPHERS": "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305",
    "AK_TLS_SNI_NAME": "ion-terra-ff.heartbeat.boo",
    "AK_TLS_VERSION": "tls1.3",
    "CLIENT_ASNUM": "7922",
    "CLIENT_CITY": "HILLSBORO",
    "CLIENT_COUNTRY": "",
    "CLIENT_COUNTRY_CODE": "US",
    "CLIENT_LAT": "45.4461",
    "CLIENT_LONG": "-122.9838",
    "CLIENT_REAL_IP": ""
}


*/




open("https://ion-terra-ff.heartbeat.boo/serverip");
var regex = '//{.*}//'; 
var jsonString = Catchpoint.extract('resp-content', regex);

try {
    setTracepoint("resp_json", jsonString);

  } catch (e) {
    console.error("Parsing error:", e);
    setTracepoint("parse_error", e.message);
  }



var regex = '//.*//'; 
open("https://ion-terra-ff.heartbeat.boo/serverip");
var headers = Catchpoint.extract('resp-headers', regex);

try {
    var jsonObject = JSON.parse(jsonString);
    for (var key in jsonObject) {
      if (jsonObject.hasOwnProperty(key)) {
        var value = jsonObject[key];
        if (value === "") {
            value = "NULL";
          }
        if (typeof value === "number" && !isNaN(value)) {
            setIndicator(key, value);
        } else {
            setTracepoint(key, value);
          }
      }
    }
  } catch (e) {
    console.error("Parsing error:", e);
    setTracepoint("PARSE_ERROR", e.message);
  }



open("https://ion-terra-ff.heartbeat.boo/serverip");
var es_hdrs = Catchpoint.extract('resp-header','CLIENT_(?s).*');
var ak_hdrs = Catchpoint.extract('resp-header','AK_(?s).*');
try {
    setTracepoint('es_hdrs', ak_hdrs);
  } catch (e) {
    console.error("Parsing error:", e);
    setTracepoint("es_hdrs", e.message);
  }
try {
    setTracepoint('ak_hdrs', ak_hdrs);
  } catch (e) {
    console.error("Parsing error:", e);
    setTracepoint("ak_hdrs", e.message);
  }



  function  headersExtract(jsRegex) {
    var hdrs = Catchpoint.extract('resp-header',jsRegex);
   
    if (typeof hdrs === "undefined" || hdrs === "") {
        var allHdrs = Catchpoint.extract('resp-header', '(?s).*');
        var hdrList = allHdrs.split("\n");
        hdrs = "";
        for (var i = 0; i < hdrList.length; i++) {
            if (hdrList[i].match(jsRegex)) {
                hdrs =  hdrs + hdrList[i] + ",";
              }
          }
    }
    if (hdrs === "") {
        hdrs = "NULL";
      }
    else {
        hdrs = "[" + hdrs.slice(0, -1) + "]";
      }

    return hdrs;
  }


open("https://ion-terra-ff.heartbeat.boo/serverip");
var es_hdrs = Catchpoint.extract('resp-header','CLIENT(?s).*');
var ak_hdrs = Catchpoint.extract('resp-header','AK_(?s).*');
var akamai_request_bc = Catchpoint.extract('resp-header','Akamai-Request-BC(?s).*');

try {
        if (es_hdrs === "" || typeof es_hdrs === "undefined") {
            es_hdrs = headersExtracrt('/CLIENT.*/gi');
        setTracepoint('es_hdrs', es_hdrs);
    }    
   
  } catch (e) {
        console.error("Parsing error:", e);
        setTracepoint("es_hdrs", e.message);
  }

try {

        if (ak_hdrs === "" || typeof ak_hdrs === "undefined") {
            ak_hdrs = headersExtracrt('/AK_.*/gi');
        
        setTracepoint('ak_hdrs', ak_hdrs);
    }    
   
  } catch (e) {
        console.error("Parsing error:", e);
        setTracepoint("ak_hdrs", e.message);
  }
  try {
        if (akamai_request_bc === "" || typeof akamai_request_bc === "undefined") {
            akamai_request_bc = headersExtracrt('/Akamai-Request-BC.*/gi');
        
        setTracepoint('akamai_request_bc', es_hdrs);
    }    
  } catch (e) {
        console.error("Parsing error:", e);
        setTracepoint("akamai_request_bc", e.message);
  }



function  headersExtract(jsRegex) {
    var hdrs = Catchpoint.extract('resp-header',jsRegex);
    
    if (typeof hdrs === "undefined" || hdrs === "") {
        var allHdrs = Catchpoint.extract('resp-header', '(?s).*');
        var hdrList = allHdrs.split("\n");
        hdrs = "";
        for (var i = 0; i < hdrList.length; i++) {
            if (hdrList[i].match(jsRegex)) {
                hdrs =  hdrs + hdrList[i] + ",";
            }
        }
    }
    if (hdrs === "") {
        hdrs = "NULL";
    }
    else {
        hdrs = "[" + hdrs.slice(0, -1) + "]";
    }
    return hdrs;
  }
  
  
  open("https://ion-terra-ff.heartbeat.boo/serverip");
  var es_hdrs = Catchpoint.extract('resp-header','CLIENT(?s).*');
  var ak_hdrs = Catchpoint.extract('resp-header','AK_(?s).*');
  var akamai_request_bc = Catchpoint.extract('resp-header','Akamai-Request-BC(?s).*');
  
  try {
          if (es_hdrs === "" || typeof es_hdrs === "undefined") {
              es_hdrs = headersExtracrt('/CLIENT.*/gi');
          
          setTracepoint('es_hdrs', es_hdrs);
      }    
     
    } catch (e) {
          console.error("Parsing error:", e);
          setTracepoint("es_hdrs", e.message);
    }
  
  try {
  
          if (ak_hdrs === "" || typeof ak_hdrs === "undefined") {
              ak_hdrs = headersExtracrt('/AK_.*/gi');
          
          setTracepoint('ak_hdrs', ak_hdrs);
      }    
     
    } catch (e) {
          console.error("Parsing error:", e);
          setTracepoint("ak_hdrs", e.message);
    }
    try {
          if (akamai_request_bc === "" || typeof akamai_request_bc === "undefined") {
              akamai_request_bc = headersExtracrt('/Akamai-Request-BC.*/gi');
          
          setTracepoint('akamai_request_bc', akamai_request_bc);
      }    
    } catch (e) {
          console.error("Parsing error:", e);
          setTracepoint("akamai_request_bc", e.message);
    }
  