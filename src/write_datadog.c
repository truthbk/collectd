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
#include "time.h"

#if HAVE_PTHREAD_H
#include <pthread.h>
#endif

#include <curl/curl.h>

#include <glib.h> //hashtable

#ifndef WRITE_DATADOG_DEFAULT_BUFFER_SIZE
# define WRITE_DATADOG_DEFAULT_BUFFER_SIZE 4096
#endif

#include <stddef.h>
#include <stdarg.h>

#define PLUGIN_NAME "write_datadog"
#define DD_BASE_URL "https://app.datadoghq.com/api/v1"

#define DD_DOGSTATSD_DEFAULT "localhost"
#define DD_DOGSTATSD_PORT_DEFAULT 8125

#define DD_MAX_TAG_LEN 255


typedef int (*custom_fn) (char *, size_t, const data_set_t *, const value_list_t *, 
                const char * const *, int, _Bool);
struct dd_handler {
        const char * name;
        custom_fn handler_fn;
};

GHashTable * dd_custom_fn_map;

/*
 * Private variables
 */
struct wdog_callback_s
{
        char     *name;

        //DD specific
        char     *endpoint;
        char     *api_key;
        int      dogstatsd_port;
        int      n_tags;
        char     **tags;
        _Bool    element_tag;
        _Bool    use_dogstatsd;

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

int svsnprintf (char *dest, size_t n, const char *format, va_list args)
{
	int ret = 0;

	ret = vsnprintf (dest, n, format, args);
	dest[n - 1] = '\0';

	return (ret);
} /* int svsnprintf */


//JSON Stuff
#if 0
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

#define PUT_CHAR(c) do { \
  if (dst_pos >= (buffer_size - 1)) { \
    buffer[buffer_size - 1] = 0; \
    return (-ENOMEM); \
  } \
  buffer[dst_pos] = (c); \
  dst_pos++; \
} while (0)

  /* Escape special characters */
  PUT_CHAR ('"');
  for (src_pos = 0; string[src_pos] != 0; src_pos++)
  {
    if ((string[src_pos] == '"')
        || (string[src_pos] == '\\'))
    {
      PUT_CHAR ('\\');
      PUT_CHAR (string[src_pos]);
    }
    else if (string[src_pos] <= 0x001F)
      PUT_CHAR ('?');
    else
      PUT_CHAR (string[src_pos]);
  } /* for */
  PUT_CHAR ('"');
  buffer[dst_pos] = 0;

#undef PUT_CHAR

  return (0);
} /* }}} int dd_json_escape_string */
#endif

static inline int buffer_add(char * buffer, int buffer_size, size_t * offset, const char * format, ...) {
  int status;
  va_list args;

  va_start(args, format);
  status = svsnprintf (buffer + *offset, buffer_size - *offset, format, args);
  if (status < 1)
    return (-1);
  else if (((size_t) status) >= (buffer_size - *offset))
    return (-ENOMEM);
  else
    *offset += ((size_t) status);

  va_end(args);
  return (0);
}

// TODO: make sure this matches expected output.
static int extract_ddmetric_name_to_json(char *buffer, size_t buffer_size,
                const data_set_t *ds, const value_list_t *vl, int n)
{
  int status = 0;
  size_t offset = 0;
  memset (buffer, 0, buffer_size);

  status += buffer_add(buffer, buffer_size, &offset, "\"");
  status += buffer_add(buffer, buffer_size, &offset, vl->plugin);
  status += buffer_add(buffer, buffer_size, &offset, ".");
  status += buffer_add(buffer, buffer_size, &offset, vl->type);
  status += buffer_add(buffer, buffer_size, &offset, ".");
  status += buffer_add(buffer, buffer_size, &offset,
                  "%s", ds->ds[n].name);
  status += buffer_add(buffer, buffer_size, &offset, "\"");

  return !!status;

}

static int extract_ddpoints_to_json(char *buffer, size_t buffer_size,
                const data_set_t *ds, const value_list_t *vl, int n)
{
  int status = 0;
  size_t offset = 0;
  time_t t = CDTIME_T_TO_TIME_T(vl->time);

  memset (buffer, 0, buffer_size);

  status += buffer_add(buffer, buffer_size, &offset, "[");
  status += buffer_add(buffer, buffer_size, &offset, "%s", ctime(&t));
  status += buffer_add(buffer, buffer_size, &offset, ", ");
  if (ds->ds[n].type == DS_TYPE_GAUGE)
          status += buffer_add (buffer, buffer_size, &offset, JSON_GAUGE_FORMAT, vl->values[n].gauge);
  else if (ds->ds[n].type == DS_TYPE_COUNTER)
          status += buffer_add (buffer, buffer_size, &offset, "%llu", vl->values[n].counter);
  else //what do we do with the other cases? DS_TYPE_DERIVE DS_TYPE_ABSOLUTE
          status += buffer_add (buffer, buffer_size, &offset, "UNKNOWN");
  status += buffer_add(buffer, buffer_size, &offset, "]");

  return !!status;
}

static int extract_ddtags_to_json(char *buffer, size_t buffer_size,
                const data_set_t *ds, const value_list_t *vl,
                const char * const * tags, int n_tags, _Bool device_tag)
{
  int i, status = 0;
  size_t offset = 0;
  memset (buffer, 0, buffer_size);

  status += buffer_add(buffer, buffer_size, &offset, "[");
  for(i=0 ; i<n_tags ; i++) {
          status += buffer_add (buffer, buffer_size, &offset, tags[i]);
          if (i != n_tags) {
                  status += buffer_add (buffer, buffer_size, &offset, ", ");
          }
  }
  if (device_tag) {
      status += buffer_add(buffer, buffer_size, &offset, "snmp_device:%s",vl->plugin_instance);
      // buffer_add("snmp_device:%s",vl->host);
  }
  status += buffer_add(buffer, buffer_size, &offset, "]");

  return !!status;
}

static int gen_dd_payload_js (char *buffer, size_t buffer_size, /* {{{ */
                const data_set_t *ds, const value_list_t *vl,
                const char * const * tags, int n_tags, _Bool device_tag)
{
  char temp[1024];
  size_t offset = 0;
  int i, status = 0;

  memset (buffer, 0, buffer_size);

  for(i=0 ; i<vl->values_len ; i++)
  {
        /* All value lists have a leading comma. The first one will be replaced with
         * a square bracket in `format_dd_json_finalize'. */
        status += buffer_add (buffer, buffer_size, &offset, ",{");
        if (status)
                return (status);

        status = extract_ddmetric_name_to_json (temp, sizeof (temp), ds, vl, i);
        if (status)
                return (status);

        status += buffer_add (buffer, buffer_size, &offset, "\"metric\":%s", temp);
        status += buffer_add(buffer, buffer_size, &offset, "[", temp);
        if (status)
                return (status);

        status = extract_ddpoints_to_json (temp, sizeof (temp), ds, vl, i);
        if (status)
                return (status);

        status += buffer_add (buffer, buffer_size, &offset, "],\"points\":%s", temp);
        status += buffer_add (buffer, buffer_size, &offset, ",\"type\":\"gauge\"");

        // WATCH OUT: this might not be consistent
        status += buffer_add (buffer, buffer_size, &offset, "host", vl->host);
        if (status)
                return (status);

        custom_fn dd_fn = (custom_fn) g_hash_table_lookup(dd_custom_fn_map, vl->plugin);
        if (dd_fn) {
                status = (*dd_fn)(temp, sizeof (temp), ds, vl,
                        tags, n_tags, device_tag);
        } else {
                status = extract_ddtags_to_json (temp, sizeof (temp), ds, vl,
                                tags, n_tags, device_tag);
        }
        if (status)
                return (status);

        status += buffer_add (buffer, buffer_size, &offset, "\"tags\":%s", temp);
        status += buffer_add (buffer, buffer_size, &offset, "}");
        if (status)
                return (status);
  }

  DEBUG ("format_json: gen_dd_payload_js: buffer = %s;", buffer);

  return (0);
} /* }}} int gen_dd_payload_js */

static int generate_dd_json(
    wdog_callback_t *cb,
    const data_set_t *ds, const value_list_t *vl)
{
  char * temp;
  int status;

  if ((cb == NULL) || (cb->send_buffer == NULL)
      || (ds == NULL) || (vl == NULL)) {
    return (-EINVAL);
  }

  if (cb->send_buffer_free < 3) {
    return (-ENOMEM);
  }

  // Get rid of this malloc
  if (!(temp = malloc(cb->send_buffer_free - 2))) {
    return (-ENOMEM);
  }

  status = gen_dd_payload_js (temp, sizeof (temp), ds, vl,
                  (const char * const*)cb->tags, cb->n_tags, cb->element_tag);
  if (status != 0)
    return (status);
  size_t temp_size = strlen (temp);

  memcpy (cb->send_buffer + (cb->send_buffer_fill), temp, temp_size + 1);
  (cb->send_buffer_fill) += temp_size;
  (cb->send_buffer_free) -= temp_size;
  sfree (temp);

  return (0);
} /* }}} int generate_dd_json_nocheck */


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

        //TODO: add api-key to request

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
        int i;
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
        sfree (cb->api_key);
        sfree (cb->cacert);
        sfree (cb->capath);
        sfree (cb->clientkey);
        sfree (cb->clientcert);
        sfree (cb->clientkeypass);
        sfree (cb->send_buffer);
        for(i=0 ; i<cb->n_tags ; i++) {
                sfree (cb->tags[i]);
        }
        if(cb->tags)
                sfree (cb->tags);

        sfree (cb);
} /* }}} void wdog_callback_free */


static int wdog_write_json (const data_set_t *ds, const value_list_t *vl, /* {{{ */
                wdog_callback_t *cb)
{
        int i, status;
        _Bool special_plugin_avail = 0;

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

        for (i=0 ; i<N_ENHANCED_PLUGINS ; i++) {
                if(strcasecmp(vl->plugin, DD_SUPPORTED_PLUGINS[i]) == 0) {
                        special_plugin_avail = 1;
                        break;
                }
        }

        if(!special_plugin_avail) {
                return (0); //do we have a better status?
        }

        status = generate_dd_json (cb, ds, vl);
        if (status == (-ENOMEM))
        {
                status = wdog_flush_nolock (/* timeout = */ 0, cb);
                if (status != 0)
                {
                        wdog_reset_buffer (cb);
                        pthread_mutex_unlock (&cb->send_lock);
                        return (status);
                }

                status = generate_dd_json (cb, ds, vl);
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
        sfree(tags);

        if (!(cb->tags = malloc(n*sizeof(char *)))) {
                cb->tags = NULL;
                return -1; //FIX: create error codes
        }

        tags = strdup(tag_str);
        n = 0;
        while((tok = strsep(&tags,", "))) {
                cb->tags[n++] = strdup(tok);
                return -1;
        }
        sfree(tags);

        return 0;
}

static int wdog_config_node (oconfig_item_t *ci) /* {{{ */
{
        wdog_callback_t *cb;
        int buffer_size = 0;
        user_data_t user_data;
        char callback_name[DATA_MAX_NAME_LEN];
        char * tag_buff = NULL;
        int i;

        cb = malloc (sizeof (*cb));
        if (cb == NULL)
        {
                ERROR (PLUGIN_NAME " plugin: malloc failed.");
                return (-1);
        }
        memset (cb, 0, sizeof (*cb));
        cb->element_tag = 1;

        cb->verify_peer = 1;
        cb->verify_host = 1;
        cb->sslversion = CURL_SSLVERSION_DEFAULT;
        cb->low_speed_limit = 0;
        cb->dogstatsd_port = DD_DOGSTATSD_PORT_DEFAULT;

        cb->timeout = 0;
        cb->log_http_error = 0;

        pthread_mutex_init (&cb->send_lock, /* attr = */ NULL);

        cf_util_get_string (ci, &cb->name);
        strncpy(cb->endpoint, DD_BASE_URL, strlen(DD_BASE_URL));

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
                else if (strcasecmp ("Use_Dogstatsd", child->key) == 0) {
                        cf_util_get_boolean (child, &cb->use_dogstatsd);

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

        // free up resources
        sfree(tag_buff);

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

// Handler list...
#define N_ENHANCED_PLUGINS 1
struct dd_handler DD_HANDLERS[] = {
        { "snmp", extract_ddtags_to_json }
}


static int wdog_init (void) /* {{{ */
{
        /* Call this while collectd is still single-threaded to avoid
         * initialization issues in libgcrypt. */
        int i = 0;
        curl_global_init (CURL_GLOBAL_SSL);
        dd_custom_fn_map =  g_hash_table_new(g_str_hash, g_str_equal);

        for (i=0 ; i<N_ENHANCED_PLUGINS ; i++){
                g_hash_table_insert(
                                dd_custom_fn_map,
                                DD_HANDLERS[i].name,
                                DD_HANDLERS[i].handler_fn
                                );
        }
        return (0);
} /* }}} int wdog_init */

void module_register (void) /* {{{ */
{
        plugin_register_complex_config ("write_datadog", wdog_config);
        plugin_register_init ("write_datadog", wdog_init);
} /* }}} void module_register */

/* vim: set fdm=marker sw=8 ts=8 tw=78 et : */
