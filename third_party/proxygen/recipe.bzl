load("@rules_cc//cc:defs.bzl", "cc_library")
load("@bazel_skylib//rules:run_binary.bzl", "run_binary")

package(
    default_visibility = ["//visibility:public"],
)

genrule(
    name = "gperf_http_headers",
    srcs = [
        "LICENSE",
        "proxygen/lib/http/gen_HTTPCommonHeaders.sh",
        "proxygen/lib/http/HTTPCommonHeaders.txt",
        "proxygen/lib/utils/gen_perfect_hash_table.sh",
        "proxygen/lib/utils/perfect_hash_table_template.h",
        "proxygen/lib/utils/perfect_hash_table_template.cpp.gperf",
    ],
    outs = [
        "proxygen/lib/http/HTTPCommonHeaders.h",
        "proxygen/lib/http/HTTPCommonHeaders.cpp",
    ],
    cmd = """
        $(execpath proxygen/lib/http/gen_HTTPCommonHeaders.sh) \
            $(execpath proxygen/lib/http/HTTPCommonHeaders.txt) \
            $$(dirname $(execpath LICENSE)) \
            $(RULEDIR)/proxygen/lib/http
    """
)

genrule(
    name = "trace_events",
    srcs = [
        "LICENSE",
        "proxygen/lib/utils/gen_trace_event_constants.py",
        "proxygen/lib/utils/samples/TraceEventType.txt",
        "proxygen/lib/utils/samples/TraceFieldType.txt",
    ],
    outs = [
        "proxygen/lib/utils/TraceEventType.h",
        "proxygen/lib/utils/TraceEventType.cpp",
        "proxygen/lib/utils/TraceFieldType.h",
        "proxygen/lib/utils/TraceFieldType.cpp",
    ],
    cmd = """
        input1=$(execpath proxygen/lib/utils/samples/TraceEventType.txt)
        input2=$(execpath proxygen/lib/utils/samples/TraceFieldType.txt)
        $(execpath proxygen/lib/utils/gen_trace_event_constants.py) \
            --output_type=cpp \
            --input_files=$$input1,$$input2 \
            --output_scope=proxygen \
            --header_path=proxygen/lib/utils \
            --install_dir=$$(dirname $(OUTS)[0]) \
            --fbcode_dir=$$(dirname $(execpath LICENSE))
    """
)

# Taken from proxygen/lib/CMakeLists.txt
proxygen_sources = [
    "proxygen/lib/healthcheck/ServerHealthCheckerCallback.cpp",
    "proxygen/lib/http/HTTP3ErrorCode.cpp",
    "proxygen/lib/http/Window.cpp",
    "proxygen/lib/http/codec/CodecProtocol.cpp",
    "proxygen/lib/http/codec/CodecUtil.cpp",
    "proxygen/lib/http/codec/compress/HeaderIndexingStrategy.cpp",
    "proxygen/lib/http/codec/compress/HeaderTable.cpp",
    "proxygen/lib/http/codec/compress/HPACKCodec.cpp",
    "proxygen/lib/http/codec/compress/HPACKContext.cpp",
    "proxygen/lib/http/codec/compress/HPACKDecodeBuffer.cpp",
    "proxygen/lib/http/codec/compress/HPACKDecoderBase.cpp",
    "proxygen/lib/http/codec/compress/HPACKDecoder.cpp",
    "proxygen/lib/http/codec/compress/HPACKEncodeBuffer.cpp",
    "proxygen/lib/http/codec/compress/HPACKEncoderBase.cpp",
    "proxygen/lib/http/codec/compress/HPACKEncoder.cpp",
    "proxygen/lib/http/codec/compress/HPACKHeader.cpp",
    "proxygen/lib/http/codec/compress/Huffman.cpp",
    "proxygen/lib/http/codec/compress/Logging.cpp",
    "proxygen/lib/http/codec/compress/NoPathIndexingStrategy.cpp",
    "proxygen/lib/http/codec/compress/QPACKCodec.cpp",
    "proxygen/lib/http/codec/compress/QPACKContext.cpp",
    "proxygen/lib/http/codec/compress/QPACKDecoder.cpp",
    "proxygen/lib/http/codec/compress/QPACKEncoder.cpp",
    "proxygen/lib/http/codec/compress/QPACKHeaderTable.cpp",
    "proxygen/lib/http/codec/compress/QPACKStaticHeaderTable.cpp",
    "proxygen/lib/http/codec/compress/StaticHeaderTable.cpp",
    "proxygen/lib/http/codec/DefaultHTTPCodecFactory.cpp",
    "proxygen/lib/http/codec/ErrorCode.cpp",
    "proxygen/lib/http/codec/FlowControlFilter.cpp",
    "proxygen/lib/http/codec/HeaderDecodeInfo.cpp",
    "proxygen/lib/http/codec/HTTP1xCodec.cpp",
    "proxygen/lib/http/codec/HTTP2Codec.cpp",
    "proxygen/lib/http/codec/HTTP2Constants.cpp",
    "proxygen/lib/http/codec/HTTP2Framer.cpp",
    "proxygen/lib/http/codec/HTTPChecks.cpp",
    "proxygen/lib/http/codec/HTTPCodecFactory.cpp",
    "proxygen/lib/http/codec/HTTPCodecFilter.cpp",
    "proxygen/lib/http/codec/HTTPCodecPrinter.cpp",
    "proxygen/lib/http/codec/HTTPParallelCodec.cpp",
    "proxygen/lib/http/codec/HTTPSettings.cpp",
    "proxygen/lib/http/codec/TransportDirection.cpp",
    "proxygen/lib/http/connpool/ServerIdleSessionController.cpp",
    "proxygen/lib/http/connpool/SessionHolder.cpp",
    "proxygen/lib/http/connpool/SessionPool.cpp",
    "proxygen/lib/http/connpool/ThreadIdleSessionController.cpp",
    "proxygen/lib/http/experimental/RFC1867.cpp",
    "proxygen/lib/http/HeaderConstants.cpp",
    "proxygen/lib/http/HTTPConnector.cpp",
    "proxygen/lib/http/HTTPConnectorWithFizz.cpp",
    "proxygen/lib/http/HTTPConstants.cpp",
    "proxygen/lib/http/HTTPException.cpp",
    "proxygen/lib/http/HTTPHeaders.cpp",
    "proxygen/lib/http/HTTPMessage.cpp",
    "proxygen/lib/http/HTTPMessageFilters.cpp",
    "proxygen/lib/http/HTTPMethod.cpp",
    "proxygen/lib/http/HTTPPriorityFunctions.cpp",
    "proxygen/lib/http/StatusTypeEnum.cpp",
    "proxygen/lib/http/ProxygenErrorEnum.cpp",
    "proxygen/lib/http/ProxyStatus.cpp",
    "proxygen/lib/http/RFC2616.cpp",
    "proxygen/lib/http/sink/HTTPTransactionSink.cpp",
    "proxygen/lib/http/observer/HTTPSessionObserverInterface.cpp",
    "proxygen/lib/http/session/ByteEvents.cpp",
    "proxygen/lib/http/session/ByteEventTracker.cpp",
    "proxygen/lib/http/session/CodecErrorResponseHandler.cpp",
    "proxygen/lib/http/session/HTTP2PriorityQueue.cpp",
    "proxygen/lib/http/session/HTTPDefaultSessionCodecFactory.cpp",
    "proxygen/lib/http/session/HTTPDirectResponseHandler.cpp",
    "proxygen/lib/http/session/HTTPDownstreamSession.cpp",
    "proxygen/lib/http/session/HTTPErrorPage.cpp",
    "proxygen/lib/http/session/HTTPEvent.cpp",
    "proxygen/lib/http/session/HTTPSessionAcceptor.cpp",
    "proxygen/lib/http/session/HTTPSessionActivityTracker.cpp",
    "proxygen/lib/http/session/HTTPSessionBase.cpp",
    "proxygen/lib/http/session/HTTPSession.cpp",
    "proxygen/lib/http/session/HTTPTransaction.cpp",
    "proxygen/lib/http/session/HTTPTransactionEgressSM.cpp",
    "proxygen/lib/http/session/HTTPTransactionIngressSM.cpp",
    "proxygen/lib/http/session/HTTPUpstreamSession.cpp",
    "proxygen/lib/http/session/SecondaryAuthManager.cpp",
    "proxygen/lib/http/session/SimpleController.cpp",
    "proxygen/lib/http/structuredheaders/StructuredHeadersBuffer.cpp",
    "proxygen/lib/http/structuredheaders/StructuredHeadersDecoder.cpp",
    "proxygen/lib/http/structuredheaders/StructuredHeadersEncoder.cpp",
    "proxygen/lib/http/structuredheaders/StructuredHeadersUtilities.cpp",
    "proxygen/lib/pools/generators/FileServerListGenerator.cpp",
    "proxygen/lib/pools/generators/ServerListGenerator.cpp",
    "proxygen/lib/sampling/Sampling.cpp",
    "proxygen/lib/services/RequestWorkerThread.cpp",
    "proxygen/lib/services/Service.cpp",
    "proxygen/lib/services/WorkerThread.cpp",
    "proxygen/lib/stats/ResourceStats.cpp",
    "proxygen/lib/transport/PersistentFizzPskCache.cpp",
    "proxygen/lib/utils/AsyncTimeoutSet.cpp",
    "proxygen/lib/utils/CryptUtil.cpp",
    "proxygen/lib/utils/Exception.cpp",
    "proxygen/lib/utils/HTTPTime.cpp",
    "proxygen/lib/utils/Logging.cpp",
    "proxygen/lib/utils/ParseURL.cpp",
    "proxygen/lib/utils/RendezvousHash.cpp",
    "proxygen/lib/utils/Time.cpp",
    "proxygen/lib/utils/TraceEventContext.cpp",
    "proxygen/lib/utils/TraceEvent.cpp",
    "proxygen/lib/utils/WheelTimerInstance.cpp",
    "proxygen/lib/utils/ZlibStreamCompressor.cpp",
    "proxygen/lib/utils/ZlibStreamDecompressor.cpp",
    "proxygen/lib/utils/ZstdStreamCompressor.cpp",
    "proxygen/lib/utils/ZstdStreamDecompressor.cpp",
    # Generated
    "proxygen/lib/http/HTTPCommonHeaders.cpp",
    "proxygen/lib/utils/TraceEventType.cpp",
    "proxygen/lib/utils/TraceFieldType.cpp",
]

proxygen_headers = [
    "proxygen/lib/http/codec/compress/CompressionInfo.h",
    "proxygen/lib/http/codec/compress/HeaderCodec.h",
    "proxygen/lib/http/codec/compress/Header.h",
    "proxygen/lib/http/codec/compress/HeaderPiece.h",
    "proxygen/lib/http/codec/compress/HPACKConstants.h",
    "proxygen/lib/http/codec/compress/HPACKHeaderName.h",
    "proxygen/lib/http/codec/compress/HPACKStreamingCallback.h",
    "proxygen/lib/http/codec/ControlMessageRateLimitFilter.h",
    "proxygen/lib/http/codec/HTTPCodec.h",
    "proxygen/lib/http/codec/HTTPRequestVerifier.h",
    "proxygen/lib/http/codec/SettingsId.h",
    "proxygen/lib/http/connpool/Endpoint.h",
    "proxygen/lib/http/HTTPHeaderSize.h",
    "proxygen/lib/http/observer/HTTPSessionObserverContainer.h",
    "proxygen/lib/http/session/AckLatencyEvent.h",
    "proxygen/lib/http/session/HTTPSessionController.h",
    "proxygen/lib/http/session/HTTPSessionStats.h",
    "proxygen/lib/http/session/SecondaryAuthManagerBase.h",
    "proxygen/lib/http/session/TransactionByteEvents.h",
    "proxygen/lib/http/session/TTLBAStats.h",
    "proxygen/lib/http/sink/HTTPSink.h",
    "proxygen/lib/http/structuredheaders/StructuredHeadersConstants.h",
    "proxygen/lib/pools/generators/MemberGroupConfig.h",
    "proxygen/lib/services/AcceptorConfiguration.h",
    "proxygen/lib/services/HTTPAcceptor.h",
    "proxygen/lib/services/ServiceWorker.h",
    "proxygen/lib/stats/PeriodicStatsDataBase.h",
    "proxygen/lib/stats/PeriodicStats.h",
    "proxygen/lib/stats/ResourceData.h",
    "proxygen/lib/utils/AcceptorAddress.h",
    "proxygen/lib/utils/CompressionFilterUtils.h",
    "proxygen/lib/utils/ConsistentHash.h",
    "proxygen/lib/utils/Export.h",
    "proxygen/lib/utils/FilterChain.h",
    "proxygen/lib/utils/StateMachine.h",
    "proxygen/lib/utils/StreamCompressor.h",
    "proxygen/lib/utils/StreamDecompressor.h",
    "proxygen/lib/utils/TraceEventObserver.h",
    "proxygen/lib/utils/UtilInl.h",
] + [
    src.removesuffix(".cpp")+".h"
    for src in proxygen_sources
]

proxygen_sources += glob(["proxygen/httpserver/*.cpp"])
proxygen_headers += glob([
    "proxygen/httpserver/*.h",
    "proxygen/httpserver/filters/*.h",
])

cc_library(
    name = "http_parser",
    hdrs = ["proxygen/external/http_parser/http_parser.h"],
    srcs = ["proxygen/external/http_parser/http_parser_cpp.cpp"],
    copts = ["-Wno-implicit-fallthrough"],
    local_defines = ["HTTP_PARSER_STRICT_URL=1"],
)

cc_library(
    name = "proxygen",
    includes = ["."],
    hdrs = proxygen_headers,
    srcs = proxygen_sources,
    deps = [
        ":http_parser",
        "@com_github_facebook_folly//:folly",
        "@com_github_facebookincubator_fizz//:fizz",
        "@com_github_facebook_wangle//:wangle",
    ]
)
