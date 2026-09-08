# VOD Batch Pull Upload Script

## Version Information

- **Current Version**: v2.0 (Supports custom path version)
- **Python Requirement**: 3.6+
- **Dependencies**: tencentcloud-sdk-python (latest version)
- **Last Updated**: January 2026

## Feature Overview

- ✅ **Batch Processing**: Read multiple URLs from a list file for batch processing
- ✅ **Concurrency Control**: Use thread pool for concurrent execution, maximum concurrency is 10
- ✅ **Retry Mechanism**: Supports up to 3 retries, using exponential backoff strategy (max 30 seconds interval)
- ✅ **Rate Limiting**: Maximum 5 requests per second, using sliding window algorithm for precise control
- ✅ **Timeout Control**: Dual-layer timeout protection (internal 60 seconds + external 70 seconds forced timeout)
- ✅ **Thread Safety**: Use independent lock mechanism to ensure concurrency safety
- ✅ **Client Reuse**: Each Worker thread independently maintains a VOD client, avoiding repeated creation
- ✅ **Smart Retry**: Distinguish between retryable and non-retryable errors
- ✅ **Progress Display**: Real-time display of task progress, success rate, and retry counts
- ✅ **Detailed Logging**: Automatically generate log files with timestamps
- ✅ **Result Saving**: Complete JSON format result report, including detailed statistical analysis
- ✅ **Parameter Validation**: Strict configuration parameter validation and URL format checking
- ✅ **Custom Path**: Support MediaStoragePath custom storage path configuration
- ✅ **URL Path Extraction**: Support automatically extracting path from URL as storage path

## File Structure

```
batch_pull_upload/
├── batch_pull_upload_api.sh   # Script to install Tencent Cloud API Python SDK
├── batch_pull_upload.py      # Main script file (core implementation)
├── config.json              # Configuration file (Tencent Cloud API Key)
├── test_urls.txt           # URL list example file
└── README.md               # Documentation (this file)
```

**Generated Files:**
- `pull_upload_YYYYMMDD_HHMMSS.log` - Execution log file
- `pull_upload_result_YYYYMMDD_HHMMSS.json` - Detailed result report

## Usage

### 0. Install Cloud API Python SDK
Go to the directory where the script is located and execute the following command:

```bash
bash batch_pull_upload_api.sh
```

### 1. Prepare URL List File

Create a text file, supporting multiple formats, one task per line, supporting comment lines:

**Format Description:**
- **Column Separator**: Use English comma `,` to separate columns
- **Column 1 (Required)**: URL address, must start with `http://` or `https://`
- **Column 2 (Optional)**: MediaName, supports Chinese, leave blank to use default name
- **Column 3 (Optional)**: ClassId, numeric format, leave blank to use default class
- **Column 4 (Optional)**: MediaStoragePath, custom storage path, starting with `/`, only sub-applications in FileID + Path mode can specify the storage path.
- **Column 5 (Optional)**: [SourceContext](https://cloud.tencent.com/document/product/266/35575#SourceContext), source context passed through as-is, up to 250 characters (returned via the [`PullComplete` event notification](https://cloud.tencent.com/document/product/266/7831) after pull upload completes). Can be used to map the uploaded media to the original vid / business-side identifier. The value must not contain commas; if the original text contains commas, pass a base64-encoded value (the tool does not encode/decode).

**URL Format Requirements:**
- Must start with `http://` or `https://`
- Supports all valid HTTP/HTTPS URL formats
- Each line can only contain one URL task
- File encoding must be UTF-8 (supports Chinese media names)

#### 🎯 Format 1: Pure URL Format
```
# This is a comment line (starts with #, will be ignored)
https://example.com/video1.mp4
https://example.com/video2.mp4

# Empty lines will be automatically skipped
https://example.com/video3.mp4
```

#### 🎯 Format 2: URL + Media Name Format
```
https://example.com/video1.mp4,MyVideo1,
https://example.com/video2.mp4,TestVideo,
```

#### 🎯 Format 3: URL + Class ID Format
```
https://example.com/video1.mp4,,1001
https://example.com/video2.mp4,,1002
```

#### 🎯 Format 4: Complete Five-Column Format
```
https://example.com/video1.mp4,MyVideo1,1001,/custom/path/video1.mp4,ctx-001
https://example.com/video2.mp4,TestVideo,1002,/videos/2024/test.mp4,ctx-002
```

#### 🎯 Format 5: Mixed Use (Fully Supported)
```
# Pure URL
https://example.com/video1.mp4

# URL + Name
https://example.com/video2.mp4,VideoName,

# URL + Class ID
https://example.com/video3.mp4,,1003

# Complete Format
https://example.com/video4.mp4,CompleteVideo,1004,/archive/video4.mp4
```

### 2. Prepare Configuration File
The script will automatically read the configuration file in the script directory: `config.json`

**Configuration File Example:**
```json
{
    "secret_id": "your_secret_id_here",
    "secret_key": "your_secret_key_here", 
    "region": "ap-guangzhou",
    "subappid": 0,
    "tasks_priority": 0,
    "procedure": "",
    "storage_region": "",
    "custom_path": {
        "use_url_path": false,
        "prefix": ""
    }
}
```

### 3. Run the Script

```bash
python3 batch_pull_upload.py your_url_list.txt
```

### 4. View Results

**Real-time Monitoring:**
- Console displays progress bar and task status in real-time
- Automatically records the number of successful/failed tasks
- Displays retry counts and execution time

**Result Files:**
- `pull_upload_YYYYMMDD_HHMMSS.log` - Detailed execution log
- `pull_upload_result_YYYYMMDD_HHMMSS.json` - Complete result report

## Configuration Description

**Configuration Validation Mechanism:**
- ✅ Automatically check if the configuration file exists
- ✅ Validate if the JSON format is correct
- ✅ Validate if all required fields exist and are not empty
- ✅ Validate field types (subappid and tasks_priority must be numbers)
- ✅ Provide detailed error prompt information

**Required Configuration Items:**
- `secret_id` - Tencent Cloud API SecretId
- `secret_key` - Tencent Cloud API SecretKey
- `region` - VOD service region (e.g., ap-guangzhou)
- `subappid` - Sub-application ID (default is "0")

**Optional Configuration Items:**
- `tasks_priority` - Task priority (optional, default is 0)
  - The larger the value, the higher the priority
  - Range: usually -10 to 10, please refer to Tencent Cloud documentation for details
  - Example: `"tasks_priority": 5` indicates high priority

- `procedure` - Task flow template name (optional, default is empty string)
  - Used to specify the processing flow after upload
  - Needs to be created in advance in the Tencent Cloud console
  - Example: `"procedure": "MyCustomProcess"` specifies a custom processing flow

- `storage_region` - Specify upload region (optional, default is empty string, which means using the account's default region)
  - Only applicable to users with special requirements for the upload region. Maps to the `StorageRegion` field of the PullUpload API
  - If a region is specified, please first enable the corresponding storage region in the console [Upload Storage Settings](https://www.tencentcloud.com/document/product/266/14059); otherwise, the API will return `InvalidParameter.StorageRegion`
  - Examples: `"storage_region": "ap-beijing"`, `"ap-chongqing"`, `"ap-singapore"`, etc.

- `custom_path` - Custom storage path configuration (optional)
  - `use_url_path`: Whether to use the URL path (default false)
  - `prefix`: Path prefix (e.g., `/videos/2024`)

## Custom Path Configuration Details

### Path Combination Priority
1. **Use URL Path**: When `use_url_path=true`, automatically extract the path part from the URL, ignoring the fourth column MediaStoragePath.
2. **Custom Path**: When `use_url_path=false` and MediaStoragePath is provided, use the path specified by the user.
3. **Path Prefix**: If prefix is configured, this prefix will be added before the path. If there is only a prefix, `use_url_path=false`, and the fourth column MediaStoragePath is empty, it will be prefix + filename.

### Configuration Examples

**Example 1: Use URL Path**
```json
{
    "custom_path": {
        "use_url_path": true,
        "prefix": "/videos"
    }
}
```
URL: `https://example.com/path/to/video.mp4`
Final Path: `/videos/path/to/video.mp4`

**Example 2: Custom Path**
```json
{
    "custom_path": {
        "use_url_path": false,
        "prefix": "/archive"
    }
}
```
URL List: `https://example.com/video.mp4,,,/custom/path/video.mp4`
Final Path: `/archive/custom/path/video.mp4`

**Example 3: Only Use Prefix**
```json
{
    "custom_path": {
        "use_url_path": false,
        "prefix": "/uploads"
    }
}
```
URL List: `https://example.com/video.mp4`
Final Path: `/uploads/video.mp4`

## SourceContext and PullComplete Event Notification

The 5th column `SourceContext` is a source context that is **passed through as-is** and returned unchanged via the [`PullComplete` event notification](https://cloud.tencent.com/document/product/266/7831) after the URL pull upload completes, up to **250 characters**. See the [SourceContext description](https://cloud.tencent.com/document/product/266/35575#SourceContext) for the field definition.

### Typical Use Case: Mapping to the Original vid

During batch migration, you can put the **source video ID (original vid) or a business-side unique identifier** in the 5th column. After the upload completes, the `PullComplete` event notification returns this value as-is via `SourceContext`. Combined with the `TaskId` and `FileId` from the same event, you can build a "source vid ↔ new FileId" mapping for migration reconciliation.

### Prerequisite: Enable Event Notification in Advance

Obtaining `SourceContext` requires **enabling event notification in the VOD console first** (either normal callback with a callback URL, or reliable callback via pulling events). If not enabled, the `PullComplete` event will not be received, and `SourceContext` cannot be obtained. See the [event notification documentation](https://cloud.tencent.com/document/product/266/7831) and [console callback settings](https://cloud.tencent.com/document/product/266/33781) for configuration.

### How to Retrieve It in the Callback

`SourceContext` is nested under `MediaBasicInfo.SourceInfo` of the `PullComplete` event. The full path is:

```
PullCompleteEvent.MediaBasicInfo.SourceInfo.SourceContext
```

Key snippet of the callback JSON:

```json
{
    "EventType": "PullComplete",
    "PullCompleteEvent": {
        "TaskId": "125xxxxxx-Pull-f5ac8127b3b6b85cdc13f237c6005d8",
        "FileId": "14508071098244959037",
        "MediaBasicInfo": {
            "MediaUrl": "http://xxx.vod2.myqcloud.com/xxx/xxx.mp4",
            "SourceInfo": {
                "SourceType": "Upload",
                "SourceContext": "your-origin-vid"  // <- the 5th column value passed at pull upload, returned as-is
            }
        },
        "FileUrl": "http://xxx.vod2.myqcloud.com/xxx/xxx.mp4"
    }
}
```

> Notes:
> - `SourceContext` is up to 250 characters. The tool does not truncate it; overly long values will be rejected by the API.
> - If the original identifier contains commas, pass a base64-encoded value in the 5th column (the tool does not encode/decode), and base64-decode it yourself after receiving the callback.

## Core Parameter Configuration

### Built-in Default Parameters
```python
# Concurrency Control
max_workers = 10                    # Maximum concurrent threads

# Rate Limiting Settings
max_requests_per_second = 5         # Maximum requests per second

# Retry Settings
max_retries = 3                     # Maximum retry attempts
INTERNAL_TIMEOUT = 60               # Internal timeout (seconds)
EXTERNAL_TIMEOUT = 70               # External forced timeout (seconds)
```

### Custom Parameters
To adjust parameters, you can modify the corresponding constants or class initialization parameters in the script:

```python
# Adjust concurrency
uploader = BatchPullUploader(max_workers=15)

# Adjust rate limiting strategy
rate_limiter = RateLimiter(max_requests_per_second=10)

# Adjust retry strategy
worker = PullUploadWorker(max_retries=5)
```

## Output Format

### Console Real-time Output
```
2024-01-15 10:30:00 - INFO - Starting batch pull upload, max concurrent workers: 10
2024-01-15 10:30:00 - INFO - Rate limiting: max 5 requests per second  
2024-01-15 10:30:00 - INFO - Retry setting: max 3 retries with exponential backoff
2024-01-15 10:30:01 - INFO - Loaded 5 tasks from test_urls.txt
2024-01-15 10:30:01 - INFO - --------------------------------------------------------------------------------
2024-01-15 10:30:02 - INFO - [1/5] 20.0% | SUCCESS | https://example.com/video1.mp4
2024-01-15 10:30:04 - INFO - [2/5] 40.0% | SUCCESS | https://example.com/video2.mp4 (retry 2 times)
2024-01-15 10:30:06 - INFO - [3/5] 60.0% | FAILED | https://example.com/video3.mp4
...
2024-01-15 10:30:30 - INFO - ================================================================================
2024-01-15 10:30:30 - INFO - Batch pull upload completed
2024-01-15 10:30:30 - INFO - ================================================================================
2024-01-15 10:30:30 - INFO - Total tasks: 5
2024-01-15 10:30:30 - INFO - Successful tasks: 4
2024-01-15 10:30:30 - INFO - Failed tasks: 1
2024-01-15 10:30:30 - INFO - Total execution time: 28.45s
2024-01-15 10:30:30 - INFO - Success rate: 80.00%
2024-01-15 10:30:30 - INFO - Throughput: 0.18 tasks/second
2024-01-15 10:30:30 - INFO - Error breakdown:
2024-01-15 10:30:30 - INFO -   INVALID_URL: 1
2024-01-15 10:30:30 - INFO - Total retry attempts: 2
2024-01-15 10:30:30 - INFO - Average task duration: 2.34s
2024-01-15 10:30:30 - INFO - Detailed results saved to: pull_upload_result_20240115_103030.json
```

### JSON Result File Format
```json
{
  "summary": {
    "total": 5,
    "success": 4,
    "failed": 1,
    "success_rate": 80.0,
    "error_breakdown": {
      "INVALID_URL": 1
    },
    "total_retries": 2,
    "average_duration": 2.34
  },
  "results": [
    {
      "line_num": 1,
      "success": true,
      "url": "https://example.com/video1.mp4",
      "media_name": "MyVideo1",
      "class_id": 1001,
      "source_context": "ctx-001",
      "response": "{\"TaskId\":\"abc123\",\"Status\":\"PROCESSING\"}",
      "duration": 2.5,
      "task_id": "abc123"
    },
    {
      "line_num": 2,
      "success": false,
      "url": "https://example.com/video2.mp4",
      "media_name": null,
      "class_id": null,
      "source_context": null,
      "error": "INVALID_URL: Invalid URL format",
      "error_code": "INVALID_URL",
      "retry_attempts": 2,
      "final_failure": true,
      "total_duration": 45.2
    }
  ]
}
```

## Error Handling Mechanism

The script uses a multi-layer error handling strategy to ensure stable operation under various abnormal conditions:

### 1. Configuration Layer Error Handling
- **File Not Found**: Automatically detect and prompt the configuration file path
- **JSON Format Error**: Detailed format validation and error location
- **Missing Parameters**: Check all required fields and provide specific missing information
- **Parameter Type Error**: Validate field types (e.g., subappid must be a number)

### 2. URL Parsing Layer Error Handling
- **File Permissions**: Handle file read permission issues
- **Encoding Issues**: Force UTF-8 encoding, handle Chinese URLs
- **URL Format Validation**: Strict URL format checking
- **Empty Lines and Comments**: Automatically skip empty lines and comment lines
- **Path Format Validation**: Check MediaStoragePath format (must start with /)

### 3. API Call Layer Error Handling
- **TencentCloudSDKException**: Catch all SDK exceptions, extract detailed error information
- **Network Timeout**: Dual-layer timeout protection (internal check + external forced)
- **Parameter Validation**: Safe parameter handling to prevent crashes caused by parameter errors
- **System Exceptions**: Catch all unexpected exceptions to ensure the program does not crash

### 4. Retry Mechanism Error Handling
- **Smart Retry Judgment**: Distinguish between retryable and non-retryable errors
  - ✅ Retryable: Network errors, API rate limiting, temporary service exceptions
  - ❌ Non-retryable: URL format errors, parameter errors, authentication failures
- **Exponential Backoff**: Retry interval increases (2^attempt, max 30 seconds)
- **Total Time Control**: Prevent infinite retries, 60 seconds total timeout

### 5. Result Saving Error Handling
- **Exception Catching**: Catch exceptions during file saving to prevent program crashes
- **Memory Management**: Clear memory data after saving attempt to prevent memory leaks
- **Error Logging**: Record warning logs when saving fails

### Error Code Description
| Error Code | Description | Retry |
|---------|------|---------|
| `INVALID_URL` | URL format error | ❌ No |
| `PARAM_ERROR` | Parameter processing error | ❌ No |
| `INTERNAL_TIMEOUT` | Internal timeout (60s) | ✅ Yes |
| `THREAD_POOL_TIMEOUT` | External forced timeout (70s) | ❌ No |
| `SYSTEM_ERROR` | System exception | ✅ Yes |
| `TASK_EXECUTION_ERROR` | Thread pool execution error | ❌ No |
| `TencentCloudSDKException` | Tencent Cloud API error | Depends on specific error |

## Usage Notes

### 🔑 Permission Requirements
- Ensure the Tencent Cloud API key has read and write permissions for the VOD service
- It is recommended to use the sub-application ID for resource isolation (`subappid` configuration)
- Monitor API quota usage to avoid exceeding call limits

### 📊 Performance Suggestions
- **Batch Size**: It is recommended to process no more than 1000 URLs in a single batch
- **Concurrency Adjustment**: Adjust `max_workers` according to server performance (5-20 is appropriate)
- **Rate Limiting Settings**: Adjust `max_requests_per_second` according to API quota
- **Network Environment**: Runs best in a stable network environment

### 🛡️ Security Notes
- The configuration file contains sensitive information, please keep it safe
- It is recommended to use temporary keys or the principle of least privilege
- Do not hardcode key information in the code
- Rotate API keys regularly

### 📝 Best Practices
1. **Test First**: Test the correctness of the configuration with a small number of URLs (1-5) first
2. **Monitor Logs**: Pay attention to error and warning information in the log file
3. **Verify Results**: Check the success rate statistics in the JSON result file
4. **Progressive Processing**: It is recommended to process large batches of tasks in batches
5. **Exception Handling**: Appropriately increase the number of retries when the network is unstable

### ⚠️ Common Issues
- **URL Format Error**: Ensure the URL starts with http:// or https://
- **File Encoding**: The URL list file must use UTF-8 encoding
- **Permission Error**: Check the read permission of the config.json file
- **Timeout Issue**: Pulling large files may require adjusting the timeout
- **Path Format Error**: MediaStoragePath must start with /

---