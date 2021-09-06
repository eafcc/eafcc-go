enum eafcc_ViewMode {
  OverlaidView,
  AllLinkedResView,
};
typedef uint32_t eafcc_ViewMode;

typedef struct eafcc_CFGCenter eafcc_CFGCenter;

typedef struct eafcc_Context eafcc_Context;

typedef struct {
  float pri;
  bool is_neg;
  char *link_path;
  char *rule_path;
  char *res_path;
} eafcc_ConfigValueReason;

typedef struct {
  char *key;
  char *content_type;
  char *value;
  eafcc_ConfigValueReason *reason;
} eafcc_ConfigValue;

const eafcc_CFGCenter *new_config_center_client(const char *cfg,
                                                void (*cb)(const void *usre_data),
                                                const void *user_data);

const eafcc_Context *new_context(const char *val);

void free_context(eafcc_Context *ctx);

eafcc_ConfigValue *get_config(const eafcc_CFGCenter *cc,
                              const eafcc_Context *ctx,
                              char **keys,
                              uintptr_t key_cnt,
                              eafcc_ViewMode view_mode,
                              uint8_t need_explain);

void free_config_value(eafcc_ConfigValue *v, uintptr_t n);
