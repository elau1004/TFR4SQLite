/* glob.c - POSIX-compatible glob() for MinGW-w64 */
#define _CRT_SECURE_NO_WARNINGS
#include "glob.h"
#include <windows.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>

/* --- Internal dynamic array --- */
typedef struct {
    char **paths;
    size_t count;
    size_t capacity;
} PathList;

/* --- Internal helpers --- */
static int  pathlist_add(PathList *pl, const char *path);
static void pathlist_free(PathList *pl);
static int  wildmatch(const char *pat, const char *str, int flags);
static int  expand_tilde(const char *in, char *out, size_t outsize);
static int  expand_brace(const char *pat, int flags, PathList *out);
static void walk_dir(const char *base, const char *relpat, int flags, int (*errfunc)(const char *, int), PathList *results);

/* ------------------------------------------------------------------ */
static int pathlist_add(PathList *pl, const char *path)
{
    if (pl->count >= pl->capacity) {
        pl->capacity = pl->capacity ? pl->capacity * 2 : 16;
        char **tmp = realloc(pl->paths, pl->capacity * sizeof(char*));
        if (!tmp) return GLOB_NOSPACE;
        pl->paths = tmp;
    }
    pl->paths[pl->count++] = _strdup(path);
    if (!pl->paths[pl->count - 1]) return GLOB_NOSPACE;
    return 0;
}

/* ------------------------------------------------------------------ */
static void pathlist_free(PathList *pl)
{
    for (size_t i = 0; i < pl->count; ++i)
        free(pl->paths[i]);
    free(pl->paths);
    pl->paths = NULL;
    pl->count = pl->capacity = 0;
}

/* ------------------------------------------------------------------ */
/* Simple wildcard: *, ?, [a-z], [!0-9] */
static int wildmatch(const char *pat, const char *str, int flags)
{
    const char *p = pat, *s = str;
    int escape = !(flags & GLOB_NOESCAPE);

    while (*p) {
        if (escape && *p == '\\') { ++p; if (*p != *s++) return 0; ++p; continue; }
        if (*p == '*') { ++p; if (!*p) return 1; while (*s) if (wildmatch(p, s++, flags)) return 1; return 0; }
        if (*p == '?') { if (!*s++) return 0; ++p; continue; }
        if (*p == '[') {
            ++p; int neg = (*p == '!'); if (neg) ++p;
            int match = 0;
            while (*p && *p != ']') { if (*p == *s) match = 1; ++p; }
            if (!*p) return 0; 
            ++p;
            if (neg) match = !match;
            if (!match) return 0;
            ++s;
            continue;
        }
        if (*p != *s) return 0;
        ++p; ++s;
    }
    return *s == '\0';
}

/* ------------------------------------------------------------------ */
static int expand_tilde(const char *in, char *out, size_t outsize)
{
    if (in[0] != '~') return (snprintf(out, outsize, "%s", in) < 0) ? -1 : 0;
    const char *rest = in + 1;
    if (*rest == '/' || *rest == '\\' || *rest == '\0') {
        char *home = getenv("USERPROFILE");
        if (!home) home = getenv("HOME");
        if (!home) return -1;
        return snprintf(out, outsize, "%s%s", home, rest);
    }
    return -1; /* ~user not supported */
}

/* ------------------------------------------------------------------ */
static int expand_brace(const char *pat, int flags, PathList *out)
{
    if (!strchr(pat, '{')) {
        char buf[MAX_PATH];
        if (expand_tilde(pat, buf, sizeof buf) != -1)
            return pathlist_add(out, buf);
        return GLOB_ABORTED;
    }

    const char *start = strchr(pat, '{');
    const char *end = strchr(start, '}');
    if (!end) return GLOB_ABORTED;

    size_t prelen = start - pat;
    char prefix[1024], suffix[1024];
    strncpy(prefix, pat, prelen); prefix[prelen] = '\0';
    strcpy(suffix, end + 1);

    const char *p = start + 1;
    while (p < end) {
        const char *comma = strchr(p, ',');
        char alt[256];
        if (comma) {
            strncpy(alt, p, comma - p); alt[comma - p] = '\0';
            p = comma + 1;
        } else {
            strcpy(alt, p); p = end;
        }

        // char full[MAX_PATH]; // 'snprintf' output between 1 and 2302 bytes into a destination of size 260
        char full[4096];
        snprintf(full, sizeof full, "%s%s%s", prefix, alt, suffix);
        char exp[MAX_PATH];
        if (expand_tilde(full, exp, sizeof exp) != -1)
            pathlist_add(out, exp);
    }
    return 0;
}

/* ------------------------------------------------------------------ */
static void walk_dir(const char *base, const char *relpat, int flags, int (*errfunc)(const char *, int), PathList *results)
{
    char spec[MAX_PATH];
    snprintf(spec, sizeof spec, "%s\\*", base);

    WIN32_FIND_DATA fd;
    HANDLE h = FindFirstFile(spec, &fd);
    if (h == INVALID_HANDLE_VALUE) {
        if ((flags & GLOB_ERR) && errfunc)
            errfunc(base, errno);
        return;
    }

    do {
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
            if (strcmp(fd.cFileName, ".") && strcmp(fd.cFileName, "..")) {
                char sub[MAX_PATH+1];
                snprintf(sub, sizeof sub, "%s\\%s", base, fd.cFileName);
                walk_dir(sub, relpat, flags, errfunc, results);
            }
            continue;
        }

        if (wildmatch(relpat, fd.cFileName, flags)) {
            char full[MAX_PATH+1];
            snprintf(full, sizeof full, "%s\\%s", base, fd.cFileName);
            if ((fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) && (flags & GLOB_MARK))
                strcat(full, "\\");
            pathlist_add(results, full);
        }
    } while (FindNextFile(h, &fd));

    FindClose(h);
}

/* ------------------------------------------------------------------ */
static int path_icmp(const void *a, const void *b) {
    return _stricmp(*(const char **)a, *(const char **)b);  // Windows case-insensitive
}  

int glob(const char *pattern, int flags, int (*errfunc)(const char *, int), glob_t *pglob)
{
    if (!pattern || !pglob) return GLOB_ABORTED;

    /* Reset output */
    pglob->gl_pathc = 0;
    pglob->gl_pathv = NULL;
    size_t offset = pglob->gl_offs;

    /* Reserve slots at start */
    if (offset) {
        pglob->gl_pathv = calloc(offset + 1, sizeof(char*));
        if (!pglob->gl_pathv) return GLOB_NOSPACE;
    }

    PathList temp = {0}, brace = {0};
    int rc;

    rc = expand_brace(pattern, flags, &brace);
    if (rc) goto cleanup;

    for (size_t i = 0; i < brace.count; ++i) {
        const char *pat = brace.paths[i];
        char dir[MAX_PATH] = ".", relpat[MAX_PATH] = "*";

        const char *sep = strrchr(pat, '/');
        const char *bs = strrchr(pat, '\\');
        if (bs > sep) sep = bs;
        if (sep) {
            size_t dlen = sep - pat;
            strncpy(dir, pat, dlen); dir[dlen] = '\0';
            strcpy(relpat, sep + 1);
        } else {
            strcpy(relpat, pat);
        }

        if (!relpat[0]) strcpy(relpat, "*");
        walk_dir(dir, relpat, flags, errfunc, &temp);
    }

    /* Sort */
    if (!(flags & GLOB_NOSORT) && temp.count > 1) {
        qsort(temp.paths, temp.count, sizeof(char*), path_icmp);
    }

    /* Copy to output */
    size_t total = offset + temp.count;
    char **final = realloc(pglob->gl_pathv, (total + 1) * sizeof(char*));
    if (!final && total) { rc = GLOB_NOSPACE; goto cleanup; }
    pglob->gl_pathv = final;

    for (size_t i = 0; i < temp.count; ++i)
        pglob->gl_pathv[offset + i] = temp.paths[i];
    pglob->gl_pathv[total] = NULL;
    pglob->gl_pathc = temp.count;

    temp.paths = NULL; temp.count = 0;

    if (!pglob->gl_pathc && !(flags & GLOB_NOCHECK)) {
        rc = GLOB_NOMATCH;
        goto cleanup;
    }

    rc = 0;

cleanup:
    pathlist_free(&temp);
    pathlist_free(&brace);
    return rc;
}

/* ------------------------------------------------------------------ */
void globfree(glob_t *pglob)
{
    if (!pglob) return;
    for (size_t i = pglob->gl_offs; i < pglob->gl_offs + pglob->gl_pathc; ++i)
        free(pglob->gl_pathv[i]);
    free(pglob->gl_pathv);
    pglob->gl_pathv = NULL;
    pglob->gl_pathc = 0;
}

#ifdef GLOB_TEST
//  gcc -o test_glob.exe -DGLOB_TEST glob.c

int main(void) {
    glob_t g = {0};
    int rc = glob("*.c", 0, NULL, &g);
    if (rc == 0) {
        for (size_t i = 0; i < g.gl_pathc; ++i)
            printf("%s\n", g.gl_pathv[i]);
        globfree(&g);
    } else {
        printf("glob() returned %d\n", rc);
    }
    return 0;
}
#endif
