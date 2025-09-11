#include "abi.h"

static const char* NAME = "yams-s3";
static const char* VER  = "0.1.0";
static const char* MANIFEST =
  "{
"
  "  \"name\": \"yams-s3\",
"
  "  \"version\": \"0.1.0\",
"
  "  \"abi\": 1,
"
  "  \"interfaces\": [
"
  "    { \"id\": \"object_storage_v1\", \"version\": 1 },
"
  "    { \"id\": \"dr_provider_v1\", \"version\": 1 }
"
  "  ],
"
  "  \"capabilities\": [\"net:aws\", \"object_lock\", \"rtc_metrics\"]
"
  "}
";

int yams_plugin_get_abi_version(void) { return 1; }
const char* yams_plugin_get_name(void) { return NAME; }
const char* yams_plugin_get_version(void) { return VER; }
const char* yams_plugin_get_manifest_json(void) { return MANIFEST; }
int yams_plugin_init(const char* config_json) { (void)config_json; return YAMS_PLUGIN_OK; }
void yams_plugin_shutdown(void) {}
int yams_plugin_get_interface(const char* iface_id, unsigned int version, void** out_iface) {
  (void)iface_id; (void)version; (void)out_iface; return YAMS_PLUGIN_ERR_NOT_FOUND; // stub
}
int yams_plugin_get_health_json(char** out_json) { (void)out_json; return YAMS_PLUGIN_ERR_NOT_FOUND; }
