set(SOURCES
  ut.c
  errinfo.c
  error.c
  strcommon.c
  digest/digest.c
  digest/digest_parser.c
  digest/param_parser.c
  contact/contact.c
  contact/parse_contact.c
  parse_event.c
  parse_expires.c
  parse_fcaps.c
  parse_fline.c
  parse_from.c
  parse_hname2.c
  parse_list_hdr.c
  parse_methods.c
  parse_min_expires.c
  parse_nameaddr.c
  parse_pai.c
  parse_param.c
  parse_ppi.c
  parse_privacy.c
  parse_refer_to.c
  parse_replaces.c
  parser_f.c
  parse_rpid.c
  parse_rr.c
  parse_sipifmatch.c
  parse_sst.c
  parse_supported.c
  parse_to.c
  parse_uri.c
  parse_via.c
  hf.c
  msg_parser.c
  parse_allow.c
  parse_authenticate.c
  parse_body.c
  parse_call_info.c
  parse_content.c
  parse_cseq.c
  parse_disposition.c
  parse_diversion.c
  )

add_library(osips_parser ${SOURCES})
set_property(TARGET osips_parser PROPERTY C_STANDARD 11)
set_property(TARGET osips_parser PROPERTY C_EXTENSIONS ON)
target_compile_options(osips_parser PRIVATE
  -Wno-sign-compare -Wno-pedantic -Wno-implicit-fallthrough)
