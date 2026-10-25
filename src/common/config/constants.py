"""Constantes y defaults de configuración."""

DEFAULT_SERVER_HOST = "0.0.0.0"
DEFAULT_SERVER_PORT = 5678
DEFAULT_MOM_HOST = "localhost"
DEFAULT_MOM_PORT = 5672

BATCH_SIZE = 1000
MAX_FRAME = 16 * 1024 * 1024
RESULT_CHUNK_SIZE = 1 * 1024 * 1024
PROGRESS_BATCH_SIZE = 100

Q1_METADATA = "Q1_Metadata"
Q2_SECTIONS = "Q2_Sections"
Q34_ABSTRACT = "Q34_Abstract"
Q5_ABSTRACT = "Q5_Abstract"

Q2_IMG_REQUEST = "Q2_Img_Request"
Q3_WORDS = "Q3_Words"
Q4_WORDS = "Q4_Words"
Q3_SHARED = "Q3_Shared"
Q4_TOP = "Q4_Top"
Q5_CANDIDATES = "Q5_Candidates"
Q5_WORDCOUNT = "Q5_WordCount"
Q5_PARTIALS = "Q5_Partials"

QUERY_RESULTS = "QueryResults"
LOGS_QUEUE = "logs"
CONTROL_EXCHANGE = "control"

CONTROL_KEY_Q1 = "q1_controller"
CONTROL_KEY_Q2_FILTER = "q2_filter"
CONTROL_KEY_IMAGE_DOWNLOADER = "image_downloader"
CONTROL_KEY_TOKENIZER = "tokenizer"
CONTROL_KEY_Q3_REDUCE = "q3_reduce"
CONTROL_KEY_Q3_JOIN = "q3_join"
CONTROL_KEY_Q4_COUNTER = "q4_counter"
CONTROL_KEY_Q4_JOIN = "q4_join"
CONTROL_KEY_Q5_COUNTER = "q5_counter"
CONTROL_KEY_Q5_ACCUMULATOR = "q5_accumulator"
CONTROL_KEY_Q5_AVG_FILTER = "q5_average_filter"

LOG_DIR = "/logs"
INPUT_FILE = "input.parquet"
