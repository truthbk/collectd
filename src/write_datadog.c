/**
 * collectd - src/write_datadog.c
 * Copyright (C) 2015 Datadog
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; only version 2 of the License is applicable.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Authors:
 *   Florian octo Forster <octo at collectd.org>
 *   Doug MacEachern <dougm@hyperic.com>
 *   Paul Sadauskas <psadauskas@gmail.com>
 **/

#include "collectd.h"
#include "plugin.h"
#include "common.h"
#include "utils_cache.h"
#include "utils_format_json.h"

#include "string.h"

#if HAVE_PTHREAD_H
# include <pthread.h>
#endif

#include <curl/curl.h>

#ifndef WRITE_DATADOG_DEFAULT_BUFFER_SIZE
# define WRITE_DATADOG_DEFAULT_BUFFER_SIZE 4096
#endif

#define PLUGIN_NAME "write_datadog"
#define DD_BASE_URL "https://app.datadoghq.com/api/v1"

#define DD_MAX_TAG_LEN 255

/*
 * Private variables
 */
struct wdog_callback_s
{
        char     *name;

        //DD specific
        char     *endpoint;
        char     *api_key;
        int      n_tags;
        char     *tags; //probably should be **
        _Bool    element_tag;

        //CURL OPTS
        _Bool    verify_peer;
        _Bool    verify_host;
        char     *cacert;
        char     *capath;
        char     *clientkey;
        char     *clientcert;
        char     *clientkeypass;
        long     sslversion;
        int      low_speed_limit;
        time_t   low_speed_time;

        _Bool    log_http_error;
        int      timeout;

        CURL     *curl;
        char     curl_errbuf[CURL_ERROR_SIZE];

        char     *send_buffer;
        size_t   send_buffer_size;
        size_t   send_buffer_free;
        size_t   send_buffer_fill;
        cdtime_t send_buffer_init_time;

        pthread_mutex_t send_lock;
};
typedef struct wdog_callback_s wdog_callback_t;


//JSON Stuff
static int dd_json_escape_string (char *buffer, size_t buffer_size, /* {{{ */
    const char *string)
{
  size_t src_pos;
  size_t dst_pos;

  if ((buffer == NULL) || (string == NULL))
    return (-EINVAL);

  if (buffer_size < 3)
    return (-ENOMEM);

  dst_pos = 0;

#define BUFFER_ADD(c) do { \
  if (dst_pos >= (buffer_size - 1)) { \
    buffer[buffer_size - 1] = 0; \
    return (-ENOMEM); \
  } \
  buffer[dst_pos] = (c); \
  dst_pos++; \
} while (0)

  /* Escape special characters */
  BUFFER_ADD ('"');
  for (src_pos = 0; string[src_pos] != 0; src_pos++)
  {
    if ((string[src_pos] == '"')
        || (string[src_pos] == '\\'))
    {
      BUFFER_ADD ('\\');
      BUFFER_ADD (string[src_pos]);
    }
    else if (string[src_pos] <= 0x001F)
      BUFFER_ADD ('?');
    else
      BUFFER_ADD (string[src_pos]);
  } /* for */
  BUFFER_ADD ('"');
  buffer[dst_pos] = 0;

#undef BUFFER_ADD

  return (0);
} /* }}} int dd_json_escape_string */

static int values_to_json (char *buffer, size_t buffer_size, /* {{{ */
                const data_set_t *ds, const value_list_t *vl, int store_rates)
{
  size_t offset = 0;
  size_t i;
  gauge_t *rates = NULL;

  memset (buffer, 0, buffer_size);

#define BUFFER_ADD(...) do { \
  int status; \
  status = ssnprintf (buffer + offset, buffer_size - offset, \
      __VA_ARGS__); \
  if (status < 1) \
  { \
    sfree(rates); \
    return (-1); \
  } \
  else if (((size_t) status) >= (buffer_size - offset)) \
  { \
    sfree(rates); \
    return (-ENOMEM); \
  } \
  else \
    offset += ((size_t) status); \
} while (0)

  BUFFER_ADD ("[");
  for (i = 0; i < ds->ds_num; i++)
  {
    if (i > 0)
      BUFFER_ADD (",");

    if (ds->ds[i].type == DS_TYPE_GAUGE)
    {
      if(isfinite (vl->values[i].gauge))
        BUFFER_ADD (JSON_GAUGE_FORMAT, vl->values[i].gauge);
      else
        BUFFER_ADD ("null");
    }
    else if (store_rates)
    {
      if (rates == NULL)
        rates = uc_get_rate (ds, vl);
      if (rates == NULL)
      {
        WARNING ("utils_format_json: uc_get_rate failed.");
        sfree(rates);
        return (-1);
      }

      if(isfinite (rates[i]))
        BUFFER_ADD (JSON_GAUGE_FORMAT, rates[i]);
      else
        BUFFER_ADD ("null");
    }
    else if (ds->ds[i].type == DS_TYPE_COUNTER)
      BUFFER_ADD ("%llu", vl->values[i].counter);
    else if (ds->ds[i].type == DS_TYPE_DERIVE)
      BUFFER_ADD ("%"PRIi64, vl->values[i].derive);
    else if (ds->ds[i].type == DS_TYPE_ABSOLUTE)
      BUFFER_ADD ("%"PRIu64, vl->values[i].absolute);
    else
    {
      ERROR ("format_json: Unknown data source type: %i",
          ds->ds[i].type);
      sfree (rates);
      return (-1);
    }
  } /* for ds->ds_num */
  BUFFER_ADD ("]");

#undef BUFFER_ADD

  DEBUG ("format_json: values_to_json: buffer = %s;", buffer);
  sfree(rates);
  return (0);
} /* }}} int values_to_json */

static int value_list_to_json (char *buffer, size_t buffer_size, /* {{{ */
                const data_set_t *ds, const value_list_t *vl, int store_rates)
{
  char temp[512];
  size_t offset = 0;
  int status;

  memset (buffer, 0, buffer_size);

#define BUFFER_ADD(...) do { \
  status = ssnprintf (buffer + offset, buffer_size - offset, \
      __VA_ARGS__); \
  if (status < 1) \
    return (-1); \
  else if (((size_t) status) >= (buffer_size - offset)) \
    return (-ENOMEM); \
  else \
    offset += ((size_t) status); \
} while (0)

  /* All value lists have a leading comma. The first one will be replaced with
   * a square bracket in `format_dd_json_finalize'. */
  BUFFER_ADD (",{");

  status = extract_ddmetric_to_json (temp, sizeof (temp), ds);
  if (status != 0)
    return (status);
  BUFFER_ADD ("\"metric\":%s", temp);

  status = extract_ddpoints_to_json (temp, sizeof (temp), ds, vl, store_rates);
  if (status != 0)
    return (status);
  BUFFER_ADD ("\"points\":%s", temp);

  status = extract_ddtypes_to_json (temp, sizeof (temp), ds);
  if (status != 0)
    return (status);
  BUFFER_ADD ("\"type\":%s", temp);

  BUFFER_ADD_KEYVAL ("host", vl->host);

  status = extract_ddtags_to_json (temp, sizeof (temp), ds);
  if (status != 0)
    return (status);
  BUFFER_ADD ("\"tags\":%s", temp);


#if 0
  status = values_to_json (temp, sizeof (temp), ds, vl, store_rates);
  if (status != 0)
    return (status);
  BUFFER_ADD ("\"values\":%s", temp);

  status = dstypes_to_json (temp, sizeof (temp), ds);
  if (status != 0)
    return (status);
  BUFFER_ADD (",\"dstypes\":%s", temp);

  status = dsnames_to_json (temp, sizeof (temp), ds);
  if (status != 0)
    return (status);
  BUFFER_ADD (",\"dsnames\":%s", temp);

  BUFFER_ADD (",\"time\":%.3f", CDTIME_T_TO_DOUBLE (vl->time));
  BUFFER_ADD (",\"interval\":%.3f", CDTIME_T_TO_DOUBLE (vl->interval));

#define BUFFER_ADD_KEYVAL(key, value) do { \
  status = dd_json_escape_string (temp, sizeof (temp), (value)); \
  if (status != 0) \
    return (status); \
  BUFFER_ADD (",\"%s\":%s", (key), temp); \
} while (0)

  BUFFER_ADD_KEYVAL ("host", vl->host);
  BUFFER_ADD_KEYVAL ("plugin", vl->plugin);
  BUFFER_ADD_KEYVAL ("plugin_instance", vl->plugin_instance);
  BUFFER_ADD_KEYVAL ("type", vl->type);
  BUFFER_ADD_KEYVAL ("type_instance", vl->type_instance);
#endif

#if 0
  if (vl->meta != NULL)
  {
    char meta_buffer[buffer_size];
    memset (meta_buffer, 0, sizeof (meta_buffer));
    status = meta_data_to_json (meta_buffer, sizeof (meta_buffer), vl->meta);
    if (status != 0)
      return (status);

    BUFFER_ADD (",\"meta\":%s", meta_buffer);
  } /* if (vl->meta != NULL) */
#endif

  BUFFER_ADD ("}");

#undef BUFFER_ADD_KEYVAL
#undef BUFFER_ADD

  DEBUG ("format_json: value_list_to_json: buffer = %s;", buffer);

  return (0);
} /* }}} int value_list_to_json */

static int format_dd_json_value_list_nocheck (char *buffer, /* {{{ */
    size_t *ret_buffer_fill, size_t *ret_buffer_free,
    const data_set_t *ds, const value_list_t *vl,
    int store_rates, size_t temp_size)
{
  char temp[temp_size];
  int status;

  status = value_list_to_json (temp, sizeof (temp), ds, vl, store_rates);
  if (status != 0)
    return (status);
  temp_size = strlen (temp);

  memcpy (buffer + (*ret_buffer_fill), temp, temp_size + 1);
  (*ret_buffer_fill) += temp_size;
  (*ret_buffer_free) -= temp_size;

  return (0);
} /* }}} int format_dd_json_value_list_nocheck */

int format_dd_json_value_list (char *buffer, /* {{{ */
    size_t *ret_buffer_fill, size_t *ret_buffer_free,
    const data_set_t *ds, const value_list_t *vl, int store_rates)
{
  if ((buffer == NULL)
      || (ret_buffer_fill == NULL) || (ret_buffer_free == NULL)
      || (ds == NULL) || (vl == NULL))
    return (-EINVAL);

  if (*ret_buffer_free < 3)
    return (-ENOMEM);

  return (format_dd_json_value_list_nocheck (buffer,
        ret_buffer_fill, ret_buffer_free, ds, vl,
        store_rates, (*ret_buffer_free) - 2));
} /* }}} int format_dd_json_value_list */



static void wdog_log_http_error (wdog_callback_t *cb)
{
        if (!cb->log_http_error)
                return;

        long http_code = 0;

        curl_easy_getinfo (cb->curl, CURLINFO_RESPONSE_CODE, &http_code);

        if (http_code != 200)
                INFO (PLUGIN_NAME " plugin: HTTP Error code: %lu", http_code);
}

static void wdog_reset_buffer (wdog_callback_t *cb)  /* {{{ */
{
        memset (cb->send_buffer, 0, cb->send_buffer_size);
        cb->send_buffer_free = cb->send_buffer_size;
        cb->send_buffer_fill = 0;
        cb->send_buffer_init_time = cdtime ();

        format_json_initialize (cb->send_buffer,
                        &cb->send_buffer_fill,
                        &cb->send_buffer_free);
} /* }}} wdog_reset_buffer */

static int wdog_send_buffer (wdog_callback_t *cb) /* {{{ */
{
        int status = 0;

        curl_easy_setopt (cb->curl, CURLOPT_POSTFIELDS, cb->send_buffer);
        status = curl_easy_perform (cb->curl);

        wdog_log_http_error (cb);

        if (status != CURLE_OK)
        {
                ERROR (PLUGIN_NAME " plugin: curl_easy_perform failed with "
                                "status %i: %s",
                                status, cb->curl_errbuf);
        }
        return (status);
} /* }}} wdog_send_buffer */

static int wdog_callback_init (wdog_callback_t *cb) /* {{{ */
{
        struct curl_slist *headers;

        if (cb->curl != NULL)
                return (0);

        cb->curl = curl_easy_init ();
        if (cb->curl == NULL)
        {
                ERROR ("curl plugin: curl_easy_init failed.");
                return (-1);
        }

        if (cb->low_speed_limit > 0 && cb->low_speed_time > 0)
        {
                curl_easy_setopt (cb->curl, CURLOPT_LOW_SPEED_LIMIT,
                                  (long) (cb->low_speed_limit * cb->low_speed_time));
                curl_easy_setopt (cb->curl, CURLOPT_LOW_SPEED_TIME,
                                  (long) cb->low_speed_time);
        }

#ifdef HAVE_CURLOPT_TIMEOUT_MS
        if (cb->timeout > 0)
                curl_easy_setopt (cb->curl, CURLOPT_TIMEOUT_MS, (long) cb->timeout);
#endif

        curl_easy_setopt (cb->curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt (cb->curl, CURLOPT_USERAGENT, COLLECTD_USERAGENT);

        headers = NULL;
        headers = curl_slist_append (headers, "Accept:  */*");
        headers = curl_slist_append (headers, "Content-Type: application/json");
        headers = curl_slist_append (headers, "Expect:");
        curl_easy_setopt (cb->curl, CURLOPT_HTTPHEADER, headers);

        curl_easy_setopt (cb->curl, CURLOPT_ERRORBUFFER, cb->curl_errbuf);
        curl_easy_setopt (cb->curl, CURLOPT_URL, cb->endpoint);
        curl_easy_setopt (cb->curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt (cb->curl, CURLOPT_MAXREDIRS, 50L);

        if (cb->user != NULL)
        {
#ifdef HAVE_CURLOPT_USERNAME
                curl_easy_setopt (cb->curl, CURLOPT_USERNAME, cb->user);
                curl_easy_setopt (cb->curl, CURLOPT_PASSWORD,
                        (cb->pass == NULL) ? "" : cb->pass);
#else
                size_t credentials_size;

                credentials_size = strlen (cb->user) + 2;
                if (cb->pass != NULL)
                        credentials_size += strlen (cb->pass);

                cb->credentials = (char *) malloc (credentials_size);
                if (cb->credentials == NULL)
                {
                        ERROR ("curl plugin: malloc failed.");
                        return (-1);
                }

                ssnprintf (cb->credentials, credentials_size, "%s:%s",
                                cb->user, (cb->pass == NULL) ? "" : cb->pass);
                curl_easy_setopt (cb->curl, CURLOPT_USERPWD, cb->credentials);
#endif
                curl_easy_setopt (cb->curl, CURLOPT_HTTPAUTH, CURLAUTH_ANY);
        }

        curl_easy_setopt (cb->curl, CURLOPT_SSL_VERIFYPEER, (long) cb->verify_peer);
        curl_easy_setopt (cb->curl, CURLOPT_SSL_VERIFYHOST,
                        cb->verify_host ? 2L : 0L);
        curl_easy_setopt (cb->curl, CURLOPT_SSLVERSION, cb->sslversion);
        if (cb->cacert != NULL)
                curl_easy_setopt (cb->curl, CURLOPT_CAINFO, cb->cacert);
        if (cb->capath != NULL)
                curl_easy_setopt (cb->curl, CURLOPT_CAPATH, cb->capath);

        if (cb->clientkey != NULL && cb->clientcert != NULL)
        {
            curl_easy_setopt (cb->curl, CURLOPT_SSLKEY, cb->clientkey);
            curl_easy_setopt (cb->curl, CURLOPT_SSLCERT, cb->clientcert);

            if (cb->clientkeypass != NULL)
                curl_easy_setopt (cb->curl, CURLOPT_SSLKEYPASSWD, cb->clientkeypass);
        }

        wdog_reset_buffer (cb);

        return (0);
} /* }}} int wdog_callback_init */

static int wdog_flush_nolock (cdtime_t timeout, wdog_callback_t *cb) /* {{{ */
{
        int status;

        DEBUG (PLUGIN_NAME " plugin: wdog_flush_nolock: timeout = %.3f; "
                        "send_buffer_fill = %zu;",
                        CDTIME_T_TO_DOUBLE (timeout),
                        cb->send_buffer_fill);

        /* timeout == 0  => flush unconditionally */
        if (timeout > 0)
        {
                cdtime_t now;

                now = cdtime ();
                if ((cb->send_buffer_init_time + timeout) > now)
                        return (0);
        }

        if (cb->send_buffer_fill <= 2)
        {
                cb->send_buffer_init_time = cdtime ();
                return (0);
        }

        status = format_json_finalize (cb->send_buffer,
                        &cb->send_buffer_fill,
                        &cb->send_buffer_free);
        if (status != 0)
        {
                ERROR ("write_datadog: wdog_flush_nolock: "
                                "format_json_finalize failed.");
                wdog_reset_buffer (cb);
                return (status);
        }

        status = wdog_send_buffer (cb);
        wdog_reset_buffer (cb);

        return (status);
} /* }}} wdog_flush_nolock */

static int wdog_flush (cdtime_t timeout, /* {{{ */
                const char *identifier __attribute__((unused)),
                user_data_t *user_data)
{
        wdog_callback_t *cb;
        int status;

        if (user_data == NULL)
                return (-EINVAL);

        cb = user_data->data;

        pthread_mutex_lock (&cb->send_lock);

        if (cb->curl == NULL)
        {
                status = wdog_callback_init (cb);
                if (status != 0)
                {
                        ERROR (PLUGIN_NAME " plugin: wdog_callback_init failed.");
                        pthread_mutex_unlock (&cb->send_lock);
                        return (-1);
                }
        }

        status = wdog_flush_nolock (timeout, cb);
        pthread_mutex_unlock (&cb->send_lock);

        return (status);
} /* }}} int wdog_flush */

static void wdog_callback_free (void *data) /* {{{ */
{
        wdog_callback_t *cb;

        if (data == NULL)
                return;

        cb = data;

        wdog_flush_nolock (/* timeout = */ 0, cb);

        if (cb->curl != NULL)
        {
                curl_easy_cleanup (cb->curl);
                cb->curl = NULL;
        }
        sfree (cb->name);
        sfree (cb->endpoint);
        sfree (cb->user);
        sfree (cb->pass);
        sfree (cb->credentials);
        sfree (cb->cacert);
        sfree (cb->capath);
        sfree (cb->clientkey);
        sfree (cb->clientcert);
        sfree (cb->clientkeypass);
        sfree (cb->send_buffer);

        sfree (cb);
} /* }}} void wdog_callback_free */

static int wdog_write_command (const data_set_t *ds, const value_list_t *vl, /* {{{ */
                wdog_callback_t *cb)
{
        char key[10*DATA_MAX_NAME_LEN];
        char values[512];
        char command[1024];
        size_t command_len;

        int status;

        if (0 != strcmp (ds->type, vl->type)) {
                ERROR (PLUGIN_NAME " plugin: DS type does not match "
                                "value list type");
                return -1;
        }

        /* Copy the identifier to `key' and escape it. */
        status = FORMAT_VL (key, sizeof (key), vl);
        if (status != 0) {
                ERROR (PLUGIN_NAME " plugin: error with format_name");
                return (status);
        }
        escape_string (key, sizeof (key));

        /* Convert the values to an ASCII representation and put that into
         * `values'. */
        status = format_values (values, sizeof (values), ds, vl, cb->store_rates);
        if (status != 0) {
                ERROR (PLUGIN_NAME " plugin: error with "
                                "wdog_value_list_to_string");
                return (status);
        }

        command_len = (size_t) ssnprintf (command, sizeof (command),
                        "PUTVAL %s interval=%.3f %s\r\n",
                        key,
                        CDTIME_T_TO_DOUBLE (vl->interval),
                        values);
        if (command_len >= sizeof (command)) {
                ERROR (PLUGIN_NAME " plugin: Command buffer too small: "
                                "Need %zu bytes.", command_len + 1);
                return (-1);
        }

        pthread_mutex_lock (&cb->send_lock);

        if (cb->curl == NULL)
        {
                status = wdog_callback_init (cb);
                if (status != 0)
                {
                        ERROR (PLUGIN_NAME " plugin: wdog_callback_init failed.");
                        pthread_mutex_unlock (&cb->send_lock);
                        return (-1);
                }
        }

        if (command_len >= cb->send_buffer_free)
        {
                status = wdog_flush_nolock (/* timeout = */ 0, cb);
                if (status != 0)
                {
                        pthread_mutex_unlock (&cb->send_lock);
                        return (status);
                }
        }
        assert (command_len < cb->send_buffer_free);

        /* `command_len + 1' because `command_len' does not include the
         * trailing null byte. Neither does `send_buffer_fill'. */
        memcpy (cb->send_buffer + cb->send_buffer_fill,
                        command, command_len + 1);
        cb->send_buffer_fill += command_len;
        cb->send_buffer_free -= command_len;

        DEBUG (PLUGIN_NAME " plugin: <%s> buffer %zu/%zu (%g%%) \"%s\"",
                        cb->endpoint,
                        cb->send_buffer_fill, cb->send_buffer_size,
                        100.0 * ((double) cb->send_buffer_fill) / ((double) cb->send_buffer_size),
                        command);

        /* Check if we have enough space for this command. */
        pthread_mutex_unlock (&cb->send_lock);

        return (0);
} /* }}} int wdog_write_command */

static int wdog_write_json (const data_set_t *ds, const value_list_t *vl, /* {{{ */
                wdog_callback_t *cb)
{
        int status;

        pthread_mutex_lock (&cb->send_lock);

        if (cb->curl == NULL)
        {
                status = wdog_callback_init (cb);
                if (status != 0)
                {
                        ERROR (PLUGIN_NAME " plugin: wdog_callback_init failed.");
                        pthread_mutex_unlock (&cb->send_lock);
                        return (-1);
                }
        }

        status = format_dd_json_value_list (cb->send_buffer,
                        &cb->send_buffer_fill,
                        &cb->send_buffer_free,
                        ds, vl, cb->store_rates);
        if (status == (-ENOMEM))
        {
                status = wdog_flush_nolock (/* timeout = */ 0, cb);
                if (status != 0)
                {
                        wdog_reset_buffer (cb);
                        pthread_mutex_unlock (&cb->send_lock);
                        return (status);
                }

                status = format_dd_json_value_list (cb->send_buffer,
                                &cb->send_buffer_fill,
                                &cb->send_buffer_free,
                                ds, vl, cb->store_rates);
        }
        if (status != 0)
        {
                pthread_mutex_unlock (&cb->send_lock);
                return (status);
        }

        DEBUG (PLUGIN_NAME " plugin: <%s> buffer %zu/%zu (%g%%)",
                        cb->endpoint,
                        cb->send_buffer_fill, cb->send_buffer_size,
                        100.0 * ((double) cb->send_buffer_fill) / ((double) cb->send_buffer_size));

        /* Check if we have enough space for this command. */
        pthread_mutex_unlock (&cb->send_lock);

        return (0);
} /* }}} int wdog_write_json */

static int wdog_write (const data_set_t *ds, const value_list_t *vl, /* {{{ */
                user_data_t *user_data)
{
        wdog_callback_t *cb;
        int status;

        if (user_data == NULL)
                return (-EINVAL);

        cb = user_data->data;
        status = wdog_write_json (ds, vl, cb);

        return (status);
} /* }}} int wdog_write */


static int config_set_tags (wdog_callback_t *cb, char *tag_str)
{
        char *tags = NULL, *tok = NULL;
        int n = 0;

        tags = strdup(tag_str);

        while((tok = strsep(&tags,", "))) {
                n++;
        }
        cb->n_tags = n;
        free(tags);

        if (!(cb->tags = malloc(n*sizeof(char *)))) {
                cb->tags = NULL;
                return -1; //FIX: create error codes
        }

        tags = strdup(tag_str);
        n = 0;
        while((tok = strsep(&tags,", "))) {
                cb->tags[n++] = strdup(tok);
                return -1
        }
        free(tags);

        return 0;
}

static int wdog_config_node (oconfig_item_t *ci) /* {{{ */
{
        wdog_callback_t *cb;
        int buffer_size = 0;
        user_data_t user_data;
        char callback_name[DATA_MAX_NAME_LEN];
        char tag_buff[DD_MAX_TAG_LEN];
        int i;

        cb = malloc (sizeof (*cb));
        if (cb == NULL)
        {
                ERROR (PLUGIN_NAME " plugin: malloc failed.");
                return (-1);
        }
        memset (cb, 0, sizeof (*cb));
        cb->tag_element = 1;

        cb->verify_peer = 1;
        cb->verify_host = 1;
        cb->sslversion = CURL_SSLVERSION_DEFAULT;
        cb->low_speed_limit = 0;

        cb->timeout = 0;
        cb->log_http_error = 0;

        pthread_mutex_init (&cb->send_lock, /* attr = */ NULL);

        cf_util_get_string (ci, &cb->name);
        cf_util_get_string (DD_BASE_URL, &cb->endpoint);

        for (i = 0; i < ci->children_num; i++)
        {
                oconfig_item_t *child = ci->children + i;

                if (strcasecmp ("API_key", child->key) == 0)
                        cf_util_get_string (child, &cb->api_key);
                else if (strcasecmp ("Tags", child->key) == 0){
                        cf_util_get_string (child, &tag_buff);
                        if(config_set_tags(cb, tag_buff)){
                                WARNING (PLUGIN_NAME " plugin: unable to parse tags. "
                                        "None will be reported - please check config..");
                        }
                }
                else if (strcasecmp ("VerifyPeer", child->key) == 0)
                        cf_util_get_boolean (child, &cb->verify_peer);
                else if (strcasecmp ("VerifyHost", child->key) == 0)
                        cf_util_get_boolean (child, &cb->verify_host);
                else if (strcasecmp ("CACert", child->key) == 0)
                        cf_util_get_string (child, &cb->cacert);
                else if (strcasecmp ("CAPath", child->key) == 0)
                        cf_util_get_string (child, &cb->capath);
                else if (strcasecmp ("ClientKey", child->key) == 0)
                        cf_util_get_string (child, &cb->clientkey);
                else if (strcasecmp ("ClientCert", child->key) == 0)
                        cf_util_get_string (child, &cb->clientcert);
                else if (strcasecmp ("ClientKeyPass", child->key) == 0)
                        cf_util_get_string (child, &cb->clientkeypass);
                else if (strcasecmp ("SSLVersion", child->key) == 0)
                {
                        char *value = NULL;

                        cf_util_get_string (child, &value);

                        if (value == NULL || strcasecmp ("default", value) == 0)
                                cb->sslversion = CURL_SSLVERSION_DEFAULT;
                        else if (strcasecmp ("SSLv2", value) == 0)
                                cb->sslversion = CURL_SSLVERSION_SSLv2;
                        else if (strcasecmp ("SSLv3", value) == 0)
                                cb->sslversion = CURL_SSLVERSION_SSLv3;
                        else if (strcasecmp ("TLSv1", value) == 0)
                                cb->sslversion = CURL_SSLVERSION_TLSv1;
#if (LIBCURL_VERSION_MAJOR > 7) || (LIBCURL_VERSION_MAJOR == 7 && LIBCURL_VERSION_MINOR >= 34)
                        else if (strcasecmp ("TLSv1_0", value) == 0)
                                cb->sslversion = CURL_SSLVERSION_TLSv1_0;
                        else if (strcasecmp ("TLSv1_1", value) == 0)
                                cb->sslversion = CURL_SSLVERSION_TLSv1_1;
                        else if (strcasecmp ("TLSv1_2", value) == 0)
                                cb->sslversion = CURL_SSLVERSION_TLSv1_2;
#endif
                        else
                                ERROR (PLUGIN_NAME " plugin: Invalid SSLVersion "
                                                "option: %s.", value);

                        sfree(value);
                }
                else if (strcasecmp ("BufferSize", child->key) == 0)
                        cf_util_get_int (child, &buffer_size);
                else if (strcasecmp ("Timeout", child->key) == 0)
                        cf_util_get_int (child, &cb->timeout);
                else if (strcasecmp ("LogHttpError", child->key) == 0)
                        cf_util_get_boolean (child, &cb->log_http_error);
                else
                {
                        ERROR (PLUGIN_NAME " plugin: Invalid configuration "
                                        "option: %s.", child->key);
                }
        }

        if (cb->low_speed_limit > 0)
                cb->low_speed_time = CDTIME_T_TO_TIME_T(plugin_get_interval());

        /* Determine send_buffer_size. */
        cb->send_buffer_size = WRITE_DATADOG_DEFAULT_BUFFER_SIZE;
        if (buffer_size >= 1024)
                cb->send_buffer_size = (size_t) buffer_size;
        else if (buffer_size != 0)
                ERROR (PLUGIN_NAME " plugin: Ignoring invalid BufferSize setting (%d).",
                                buffer_size);

        /* Allocate the buffer. */
        cb->send_buffer = malloc (cb->send_buffer_size);
        if (cb->send_buffer == NULL)
        {
                ERROR (PLUGIN_NAME " plugin: malloc(%zu) failed.", cb->send_buffer_size);
                wdog_callback_free (cb);
                return (-1);
        }
        /* Nulls the buffer and sets ..._free and ..._fill. */
        wdog_reset_buffer (cb);

        ssnprintf (callback_name, sizeof (callback_name), "write_datadog/%s",
                        cb->name);
        DEBUG ("write_datadog: Registering write callback '%s' with URL '%s'",
                        callback_name, cb->endpoint);

        memset (&user_data, 0, sizeof (user_data));
        user_data.data = cb;
        user_data.free_func = NULL;
        plugin_register_flush (callback_name, wdog_flush, &user_data);

        user_data.free_func = wdog_callback_free;
        plugin_register_write (callback_name, wdog_write, &user_data);

        return (0);
} /* }}} int wdog_config_node */

static int wdog_config (oconfig_item_t *ci) /* {{{ */
{
        int i;

        for (i = 0; i < ci->children_num; i++)
        {
                oconfig_item_t *child = ci->children + i;

                if (strcasecmp ("Node", child->key) == 0)
                        wdog_config_node (child);
                /* FIXME: Remove this legacy mode in version 6. */
                else if (strcasecmp ("URL", child->key) == 0) {
                        WARNING (PLUGIN_NAME " plugin: Legacy <URL> block found. "
                                "Please use <Node> instead.");
                        wdog_config_node (child);
                }
                else
                {
                        ERROR (PLUGIN_NAME " plugin: Invalid configuration "
                                        "option: %s.", child->key);
                }
        }

        return (0);
} /* }}} int wdog_config */

static int wdog_init (void) /* {{{ */
{
        /* Call this while collectd is still single-threaded to avoid
         * initialization issues in libgcrypt. */
        curl_global_init (CURL_GLOBAL_SSL);
        return (0);
} /* }}} int wdog_init */

void module_register (void) /* {{{ */
{
        plugin_register_complex_config ("write_datadog", wdog_config);
        plugin_register_init ("write_datadog", wdog_init);
} /* }}} void module_register */

/* vim: set fdm=marker sw=8 ts=8 tw=78 et : */
