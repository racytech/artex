#ifndef DASHBOARD_H
#define DASHBOARD_H

#include <stdarg.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* =========================================================================
 * Limits
 * ========================================================================= */

#define DASHBOARD_MAX_SECTIONS  16
#define DASHBOARD_MAX_FIELDS    16
#define DASHBOARD_MAX_LABEL     24
#define DASHBOARD_MAX_VALUE     48
#define DASHBOARD_MAX_TITLE     24

/* =========================================================================
 * Types
 * ========================================================================= */

typedef struct {
    char label[DASHBOARD_MAX_LABEL];
    char value[DASHBOARD_MAX_VALUE];
    int  color;     /* ANSI color code (e.g. 32=green), 0=default */
    bool active;
} dashboard_field_t;

typedef struct {
    char              title[DASHBOARD_MAX_TITLE];
    dashboard_field_t fields[DASHBOARD_MAX_FIELDS];
    int               field_count;
    int               width;    /* 0 = auto-compute */
    bool              active;
} dashboard_section_t;

typedef struct {
    dashboard_section_t sections[DASHBOARD_MAX_SECTIONS];
    int  section_count;
    int  columns;       /* number of columns in grid layout */
    int  term_w;        /* terminal width  (updated on render) */
    int  term_h;        /* terminal height (updated on render) */
    bool entered;       /* true if dashboard_enter() was called */
} dashboard_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/** Initialize dashboard with given column count. */
void dashboard_init(dashboard_t *db, int columns);

/** No-op cleanup (all storage is inline). */
void dashboard_cleanup(dashboard_t *db);

/* =========================================================================
 * Terminal Control
 * ========================================================================= */

/** Switch to alternate screen buffer, hide cursor. */
void dashboard_enter(void);

/** Restore original screen, show cursor. */
void dashboard_leave(void);

/* =========================================================================
 * Sections
 * ========================================================================= */

/** Add a section. Returns section ID (index), or -1 if full. */
int dashboard_add_section(dashboard_t *db, const char *title);

/** Remove a section by ID. Shifts remaining sections down. */
void dashboard_remove_section(dashboard_t *db, int section_id);

/** Set fixed width for a section (0 = auto). */
void dashboard_set_section_width(dashboard_t *db, int section_id, int width);

/* =========================================================================
 * Fields
 * ========================================================================= */

/** Add a field to a section. Returns field ID (index), or -1 if full. */
int dashboard_add_field(dashboard_t *db, int section_id, const char *label);

/** Remove a field by ID. Shifts remaining fields down. */
void dashboard_remove_field(dashboard_t *db, int section_id, int field_id);

/* =========================================================================
 * Values
 * ========================================================================= */

/** Set a field's display value (printf-style). */
void dashboard_set(dashboard_t *db, int section_id, int field_id,
                   const char *fmt, ...);

/** Set a field's display value (va_list variant). */
void dashboard_vset(dashboard_t *db, int section_id, int field_id,
                    const char *fmt, va_list ap);

/** Set the ANSI color for a field's value (e.g. 32=green, 31=red, 0=reset). */
void dashboard_set_color(dashboard_t *db, int section_id, int field_id,
                         int ansi_color);

/* =========================================================================
 * Rendering
 * ========================================================================= */

/** Full redraw. Overwrites in place (no flicker). */
void dashboard_render(dashboard_t *db);

#ifdef __cplusplus
}
#endif

#endif /* DASHBOARD_H */
