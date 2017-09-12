from collections import defaultdict
import pyspark.sql.functions as F

VERSION = 1


def mode(l, empty_value='MISSING'):
    if not l:
        return empty_value
    counts = defaultdict(int)
    for value in l:
        counts[value] += 1
    counts = [(v, k) for (k, v) in counts.items()]
    counts.sort()
    return counts[-1][1]


_FIELD_AGGREGATORS = [
    F.sum('aborts_content').alias('aborts_content_sum'),
    F.sum('aborts_gmplugin').alias('aborts_gmplugin_sum'),
    F.sum('aborts_plugin').alias('aborts_plugin_sum'),
    # active_addons
    F.mean('active_addons_count').alias('active_addons_count_mean'),
    # MAIN_SUMMARY_FIELD_AGGREGATORS inserts here
    # active_theme
    F.sum(F.expr('active_ticks/(3600.0/5)')).alias('active_hours_sum'),
    F.first('addon_compatibility_check_enabled').alias(
        'addon_compatibility_check_enabled'),
    F.first('app_build_id').alias('app_build_id'),
    F.first('app_display_version').alias('app_display_version'),
    F.first('app_name').alias('app_name'),
    F.first('app_version').alias('app_version'),
    # attribution
    F.first('blocklist_enabled').alias('blocklist_enabled'),
    F.first('channel').alias('channel'),
    F.first('city').alias('city'),
    F.first('country').alias('country'),
    F.sum('crashes_detected_content').alias('crashes_detected_content_sum'),
    F.sum('crashes_detected_gmplugin').alias('crashes_detected_gmplugin_sum'),
    F.sum('crashes_detected_plugin').alias('crashes_detected_plugin_sum'),
    F.sum('crash_submit_attempt_content').alias(
        'crash_submit_attempt_content_sum'),
    F.sum('crash_submit_attempt_main').alias('crash_submit_attempt_main_sum'),
    F.sum('crash_submit_attempt_plugin').alias(
        'crash_submit_attempt_plugin_sum'),
    F.sum('crash_submit_success_content').alias(
        'crash_submit_success_content_sum'),
    F.sum('crash_submit_success_main').alias('crash_submit_success_main_sum'),
    F.sum('crash_submit_success_plugin').alias(
        'crash_submit_success_plugin_sum'),
    F.first('default_search_engine').alias('default_search_engine'),
    F.first('default_search_engine_data_load_path').alias(
        'default_search_engine_data_load_path'),
    F.first('default_search_engine_data_name').alias(
        'default_search_engine_data_name'),
    F.first('default_search_engine_data_origin').alias(
        'default_search_engine_data_origin'),
    F.first('default_search_engine_data_submission_url').alias(
        'default_search_engine_data_submission_url'),
    F.sum('devtools_toolbox_opened_count').alias(
        'devtools_toolbox_opened_count_sum'),
    F.first('distribution_id').alias('distribution_id'),
    # userprefs/dom_ipc_process_count
    F.first('e10s_cohort').alias('e10s_cohort'),
    F.first('e10s_enabled').alias('e10s_enabled'),
    F.first('env_build_arch').alias('env_build_arch'),
    F.first('env_build_id').alias('env_build_id'),
    F.first('env_build_version').alias('env_build_version'),
    # events
    # EXPERIMENT_FIELD_AGGREGATORS inserts here
    # experiments
    F.mean('first_paint').alias('first_paint_mean'),
    # F.first(
    #     F.expr("userprefs.extensions_allow_non_mpc_extensions"
    #        ).alias("extensions_allow_non_mpc_extensions")
    # ),
    F.first('flash_version').alias('flash_version'),
    F.first('install_year').alias('install_year'),
    F.first('is_default_browser').alias('is_default_browser'),
    F.first('is_wow64').alias('is_wow64'),
    F.first('locale').alias('locale'),
    # main
    F.first('memory_mb').alias('memory_mb'),  # mean?
    F.first('os').alias('os'),
    F.first('os_service_pack_major').alias('os_service_pack_major'),
    F.first('os_service_pack_minor').alias('os_service_pack_minor'),
    F.first('os_version').alias('os_version'),
    F.first('normalized_channel').alias('normalized_channel'),
    F.countDistinct('document_id').alias('pings_aggregated_by_this_row'),
    F.mean('places_bookmarks_count').alias('places_bookmarks_count_mean'),
    F.mean('places_pages_count').alias('places_pages_count_mean'),
    F.sum('plugin_hangs').alias('plugin_hangs_sum'),
    F.sum('plugins_infobar_allow').alias('plugins_infobar_allow_sum'),
    F.sum('plugins_infobar_block').alias('plugins_infobar_block_sum'),
    F.sum('plugins_infobar_shown').alias('plugins_infobar_shown_sum'),
    F.sum('plugins_notification_shown').alias(
        'plugins_notification_shown_sum'),
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
    F.sum('push_api_notify').alias('push_api_notify_sum'),
    F.first('sample_id').alias('sample_id'),
    F.first('scalar_parent_aushelper_websense_reg_version').alias(
        'scalar_parent_aushelper_websense_reg_version'),
    F.max('scalar_parent_browser_engagement_max_concurrent_tab_count').alias(
        'scalar_parent_browser_engagement_max_concurrent_tab_count_max'),
    F.max(
        'scalar_parent_browser_engagement_max_concurrent_window_count').alias(
        'scalar_parent_browser_engagement_max_concurrent_window_count_max'),
    # scalar_parent_browser_engagement_navigation_about_home
    # scalar_parent_browser_engagement_navigation_about_newtab
    # scalar_parent_browser_engagement_navigation_contextmenu
    # scalar_parent_browser_engagement_navigation_searchbar
    # scalar_parent_browser_engagement_navigation_urlbar
    F.sum('scalar_parent_browser_engagement_tab_open_event_count').alias(
        'scalar_parent_browser_engagement_tab_open_event_count_sum'),
    F.sum('scalar_parent_browser_engagement_total_uri_count').alias(
        'scalar_parent_browser_engagement_total_uri_count_sum'),
    F.sum('scalar_parent_browser_engagement_unfiltered_uri_count').alias(
        'scalar_parent_browser_engagement_unfiltered_uri_count_sum'),
    F.max('scalar_parent_browser_engagement_unique_domains_count').alias(
        'scalar_parent_browser_engagement_unique_domains_count_max'),
    F.sum('scalar_parent_browser_engagement_window_open_event_count').alias(
        'scalar_parent_browser_engagement_window_open_event_count_sum'),
    # F.sum('scalar_parent_browser_browser_usage_graphite').alias(
    #     'scalar_parent_browser_browser_usage_graphite_sum')
    F.sum('scalar_parent_devtools_copy_full_css_selector_opened').alias(
        'scalar_parent_devtools_copy_full_css_selector_opened_sum'),
    F.sum('scalar_parent_devtools_copy_unique_css_selector_opened').alias(
        'scalar_parent_devtools_copy_unique_css_selector_opened_sum'),
    F.sum('scalar_parent_devtools_toolbar_eyedropper_opened').alias(
        'scalar_parent_devtools_toolbar_eyedropper_opened_sum'),
    F.sum('scalar_parent_dom_contentprocess_troubled_due_to_memory').alias(
        'scalar_parent_dom_contentprocess_troubled_due_to_memory_sum'),
    F.sum('scalar_parent_navigator_storage_estimate_count').alias(
        'scalar_parent_navigator_storage_estimate_count_sum'),
    F.sum('scalar_parent_navigator_storage_persist_count').alias(
        'scalar_parent_navigator_storage_persist_count_sum'),
    F.first('scalar_parent_services_sync_fxa_verification_method').alias(
        'scalar_parent_services_sync_fxa_verification_method'),
    F.sum('scalar_parent_storage_sync_api_usage_extensions_using').alias(
        'scalar_parent_storage_sync_api_usage_extensions_using_sum'),
    # scalar_parent_storage_sync_api_usage_items_stored
    # scalar_parent_storage_sync_api_usage_storage_consumed
    F.first('scalar_parent_telemetry_os_shutting_down').alias(
        'scalar_parent_telemetry_os_shutting_down'),
    F.sum('scalar_parent_webrtc_nicer_stun_retransmits').alias(
        'scalar_parent_webrtc_nicer_stun_retransmits_sum'),
    F.sum('scalar_parent_webrtc_nicer_turn_401s').alias(
        'scalar_parent_webrtc_nicer_turn_401s_sum'),
    F.sum('scalar_parent_webrtc_nicer_turn_403s').alias(
        'scalar_parent_webrtc_nicer_turn_403s_sum'),
    F.sum('scalar_parent_webrtc_nicer_turn_438s').alias(
        'scalar_parent_webrtc_nicer_turn_438s_sum'),
    F.first('search_cohort').alias('search_cohort'),
    F.sum('search_count_all').alias('search_count_all_sum'),
    F.sum('search_count_abouthome').alias('search_count_abouthome_sum'),
    F.sum('search_count_contextmenu').alias('search_count_contextmenu_sum'),
    F.sum('search_count_newtab').alias('search_count_newtab_sum'),
    F.sum('search_count_searchbar').alias('search_count_searchbar_sum'),
    F.sum('search_count_system').alias('search_count_system_sum'),
    F.sum('search_count_urlbar').alias('search_count_urlbar_sum'),
    F.mean('session_restored').alias('session_restored_mean'),
    F.sum(F.expr("IF(subsession_counter = 1, 1, 0)")).alias(
        "sessions_started_on_this_day"),
    # shutdown_kill
    F.sum(F.expr('subsession_length/3600.0')).alias('subsession_hours_sum'),
    # ssl_handshake_result
    F.sum('ssl_handshake_result_failure').alias(
        'ssl_handshake_result_failure_sum'),
    F.sum('ssl_handshake_result_success').alias(
        'ssl_handshake_result_success_sum'),
    F.first('sync_configured').alias('sync_configured'),
    F.sum('sync_count_desktop').alias('sync_count_desktop_sum'),
    F.sum('sync_count_mobile').alias('sync_count_mobile_sum'),
    F.first('telemetry_enabled').alias('telemetry_enabled'),
    F.first('timezone_offset').alias('timezone_offset'),
    F.sum(F.expr('total_time/3600.0')).alias('total_hours_sum'),
    F.first('vendor').alias('vendor'),
    F.sum('web_notification_shown').alias('web_notification_shown_sum'),
    F.first('windows_build_number').alias('windows_build_number'),
    F.first('windows_ubr').alias('windows_ubr'),
]


MAIN_SUMMARY_FIELD_AGGREGATORS = _FIELD_AGGREGATORS[:4] + [

    F.first('active_experiment_branch').alias('active_experiment_branch'),
    F.first('active_experiment_id').alias('active_experiment_id'),
] + _FIELD_AGGREGATORS[4:]


EXPERIMENT_FIELD_AGGREGATORS = _FIELD_AGGREGATORS[:15] + [
    F.first('experiment_branch').alias('experiment_branch'),
] + _FIELD_AGGREGATORS[15:]
