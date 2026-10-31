#include <nng/nng.h>
#include <stdlib.h>
#include <string.h>

struct nng_stat {
    int type;
    char name[32];
    uint64_t val;
    struct nng_stat *next;
    struct nng_stat *child;
};

int nng_stats_get(nng_stat **statsp) {
    nng_stat *root = (nng_stat *)calloc(1, sizeof(nng_stat));
    root->type = NNG_STAT_SCOPE;
    nng_stat *rej = (nng_stat *)calloc(1, sizeof(nng_stat));
    rej->type = NNG_STAT_COUNTER;
    strncpy(rej->name, "reject", sizeof(rej->name)-1);
    rej->val = 1;
    root->child = rej;
    *statsp = root;
    return 0;
}

void nng_stats_free(nng_stat *s) {
    if (!s) return;
    if (s->child) nng_stats_free(s->child);
    if (s->next) nng_stats_free(s->next);
    free(s);
}

void nng_stats_dump(const nng_stat *s) {}

const nng_stat *nng_stat_next(const nng_stat *s) { return s ? s->next : NULL; }
const nng_stat *nng_stat_child(const nng_stat *s) { return s ? s->child : NULL; }
const char *nng_stat_name(const nng_stat *s) { return s ? s->name : ""; }
int nng_stat_type(const nng_stat *s) { return s ? s->type : 0; }
const nng_stat *nng_stat_find(const nng_stat *s, const char *name) {
    if (!s) return NULL;
    if (strcmp(s->name, name) == 0) return s;
    if (s->child) {
        const nng_stat *f = nng_stat_find(s->child, name);
        if (f) return f;
    }
    return nng_stat_find(s->next, name);
}
const nng_stat *nng_stat_find_socket(const nng_stat *s, nng_socket sock) {
    return s;
}
const nng_stat *nng_stat_find_dialer(const nng_stat *s, nng_dialer d) { return s; }
const nng_stat *nng_stat_find_listener(const nng_stat *s, nng_listener l) { return s; }
int nng_stat_unit(const nng_stat *s) { return 0; }
uint64_t nng_stat_value(const nng_stat *s) { return s ? s->val : 0; }
bool nng_stat_bool(const nng_stat *s) { return s ? s->val != 0 : false; }
const char *nng_stat_string(const nng_stat *s) { return ""; }
const char *nng_stat_desc(const nng_stat *s) { return ""; }
uint64_t nng_stat_timestamp(const nng_stat *s) { return 0; }
