import pyspark.sql.functions as F

VERSION = 1


def get_alias(field_name, alias, kind):
    field_alias = alias
    if field_alias is None:
        field_alias = '{}_{}'.format(field_name, kind)
    return field_alias


def agg_sum(field_name, alias=None, expression=None):
    field_alias = get_alias(field_name, alias, "sum")
    field_expression = expression
    if field_expression is None:
        field_expression = field_name
    return F.sum(field_expression).alias(field_alias)


def agg_mean(field_name, alias=None):
    field_alias = get_alias(field_name, alias, "mean")
    return F.mean(field_name).alias(field_alias)


def agg_first(field_name):
    return F.first(field_name).alias(field_name)


def agg_max(field_name, alias=None):
    field_alias = get_alias(field_name, alias, "max")
    return F.max(field_name).alias(field_alias)


_FIELD_AGGREGATORS = [
    agg_sum('aborts_content'),
    agg_sum('aborts_gmplugin'),
    agg_sum('aborts_plugin'),
    # active_addons
    agg_mean('active_addons_count'),
    # MAIN_SUMMARY_FIELD_AGGREGATORS inserts here

    # active_theme
    agg_sum('active_ticks', alias='active_hours_sum',
            expression=F.expr('active_ticks/(3600.0/5)')),
    agg_first('addon_compatibility_check_enabled'),
    agg_first('app_build_id'),
    agg_first('app_display_version'),
    agg_first('app_name'),
    agg_first('app_version'),
    # attribution
    agg_first('blocklist_enabled'),
    agg_first('channel'),
    agg_first('city'),
    agg_first('country'),
    agg_sum('crashes_detected_content'),
    agg_sum('crashes_detected_gmplugin'),
    agg_sum('crashes_detected_plugin'),
    agg_sum('crash_submit_attempt_content'),
    agg_sum('crash_submit_attempt_main'),
    agg_sum('crash_submit_attempt_plugin'),
    agg_sum('crash_submit_success_content'),
    agg_sum('crash_submit_success_main'),
    agg_sum('crash_submit_success_plugin'),
    agg_first('default_search_engine'),
    agg_first('default_search_engine_data_load_path'),
    agg_first('default_search_engine_data_name'),
    agg_first('default_search_engine_data_origin'),
    agg_first('default_search_engine_data_submission_url'),
    agg_sum('devtools_toolbox_opened_count'),
    agg_first('distribution_id'),
    # userprefs/dom_ipc_process_count
    agg_first('e10s_cohort'),
    agg_first('e10s_enabled'),
    agg_first('env_build_arch'),
    agg_first('env_build_id'),
    agg_first('env_build_version'),
    # events
    # EXPERIMENT_FIELD_AGGREGATORS inserts here
    # experiments
    agg_mean('first_paint'),
    # F.first(
    #     F.expr("userprefs.extensions_allow_non_mpc_extensions"
    #        ).alias("extensions_allow_non_mpc_extensions")
    # ),
    agg_first('flash_version'),
    agg_first('install_year'),
    agg_first('is_default_browser'),
    agg_first('is_wow64'),
    agg_first('locale'),
    # main
    agg_first('memory_mb'),  # mean?
    agg_first('os'),
    agg_first('os_service_pack_major'),
    agg_first('os_service_pack_minor'),
    agg_first('os_version'),
    agg_first('normalized_channel'),
    F.countDistinct('document_id').alias('pings_aggregated_by_this_row'),
    agg_mean('places_bookmarks_count'),
    agg_mean('places_pages_count'),
    agg_sum('plugin_hangs'),
    agg_sum('plugins_infobar_allow'),
    agg_sum('plugins_infobar_block'),
    agg_sum('plugins_infobar_shown'),
    agg_sum('plugins_notification_shown'),
    # plugins_notification_user_action
    # popup_notification_stats
    F.first(
        F.expr(
            "datediff(subsession_start_date, " \
            "from_unixtime(profile_creation_date*24*60*60))"
        )
    ).alias("profile_age_in_days"),
    F.first(
        F.expr("from_unixtime(profile_creation_date*24*60*60)")
    ).alias('profile_creation_date'),
    agg_sum('push_api_notify'),
    agg_first('sample_id'),
    agg_first('scalar_parent_aushelper_websense_reg_version'),
    agg_max('scalar_parent_browser_engagement_max_concurrent_tab_count'),
    agg_max('scalar_parent_browser_engagement_max_concurrent_window_count'),
    # scalar_parent_browser_engagement_navigation_about_home
    # scalar_parent_browser_engagement_navigation_about_newtab
    # scalar_parent_browser_engagement_navigation_contextmenu
    # scalar_parent_browser_engagement_navigation_searchbar
    # scalar_parent_browser_engagement_navigation_urlbar
    agg_sum('scalar_parent_browser_engagement_tab_open_event_count'),
    agg_sum('scalar_parent_browser_engagement_total_uri_count'),
    agg_sum('scalar_parent_browser_engagement_unfiltered_uri_count'),
    agg_max('scalar_parent_browser_engagement_unique_domains_count'),
    agg_sum('scalar_parent_browser_engagement_window_open_event_count'),
    # F.sum('scalar_parent_browser_browser_usage_graphite').alias(
    #     'scalar_parent_browser_browser_usage_graphite_sum')
    agg_sum('scalar_parent_devtools_copy_full_css_selector_opened'),
    agg_sum('scalar_parent_devtools_copy_unique_css_selector_opened'),
    agg_sum('scalar_parent_devtools_toolbar_eyedropper_opened'),
    agg_sum('scalar_parent_dom_contentprocess_troubled_due_to_memory'),
    agg_sum('scalar_parent_navigator_storage_estimate_count'),
    agg_sum('scalar_parent_navigator_storage_persist_count'),
    agg_first('scalar_parent_services_sync_fxa_verification_method'),
    agg_sum('scalar_parent_storage_sync_api_usage_extensions_using'),
    # scalar_parent_storage_sync_api_usage_items_stored
    # scalar_parent_storage_sync_api_usage_storage_consumed
    agg_first('scalar_parent_telemetry_os_shutting_down'),
    agg_sum('scalar_parent_webrtc_nicer_stun_retransmits'),
    agg_sum('scalar_parent_webrtc_nicer_turn_401s'),
    agg_sum('scalar_parent_webrtc_nicer_turn_403s'),
    agg_sum('scalar_parent_webrtc_nicer_turn_438s'),
    agg_first('search_cohort'),
    agg_sum('search_count_all'),
    agg_sum('search_count_abouthome'),
    agg_sum('search_count_contextmenu'),
    agg_sum('search_count_newtab'),
    agg_sum('search_count_searchbar'),
    agg_sum('search_count_system'),
    agg_sum('search_count_urlbar'),
    agg_mean('session_restored'),
    agg_sum('subsession_counter', alias='sessions_started_on_this_day',
            expression=F.expr("IF(subsession_counter = 1, 1, 0)")),
    agg_sum('shutdown_kill'),
    agg_sum('subsession_length', alias='subsession_hours_sum',
            expression=F.expr('subsession_length/3600.0')),
    # ssl_handshake_result
    agg_sum('ssl_handshake_result_failure'),
    agg_sum('ssl_handshake_result_success'),
    agg_first('sync_configured'),
    agg_sum('sync_count_desktop'),  # TODO: max
    agg_sum('sync_count_mobile'),   # TODO: max
    agg_first('telemetry_enabled'),
    agg_first('timezone_offset'),
    agg_sum('total_time', alias='total_hours_sum',
            expression=F.expr('total_time/3600.0')),
    agg_first('vendor'),
    agg_sum('web_notification_shown'),
    agg_first('windows_build_number'),
    agg_first('windows_ubr'),
]


MAIN_SUMMARY_FIELD_AGGREGATORS = _FIELD_AGGREGATORS[:4] + [
    agg_first('active_experiment_branch'),
    agg_first('active_experiment_id'),
] + _FIELD_AGGREGATORS[4:]


EXPERIMENT_FIELD_AGGREGATORS = _FIELD_AGGREGATORS[:15] + [
    agg_first('experiment_branch'),
] + _FIELD_AGGREGATORS[15:]


ACTIVITY_DATE_COLUMN = F.expr(
    "substr(subsession_start_date, 1, 10)"
).alias("activity_date")
