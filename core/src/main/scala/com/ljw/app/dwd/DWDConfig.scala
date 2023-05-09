package com.ljw.app.dwd

object DWDConfig {
  //action data
  var source_topic = "topic_log"
  var groupId = "dwd_con"
  var dirty_topic = "dirty_data"
  val page_topic = "dwd_traffic_page_log"
  val start_topic = "dwd_traffic_start_log"
  val display_topic = "dwd_traffic_display_log"
  val action_topic = "dwd_traffic_action_log"
  val error_topic = "dwd_traffic_error_log"

  val uv_topic = "traffic_unique_visitor_detail"
  val bounceRateTopic = "dwd_traffic_user_jump_detail"
}
