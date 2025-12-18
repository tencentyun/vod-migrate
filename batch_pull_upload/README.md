# 云点播批量拉取上传脚本

## 版本信息

- **当前版本**：v1.0（最新优化版本）
- **Python要求**：3.6+
- **依赖库**：tencentcloud-sdk-python
- **最后更新**：2025年12月

## 功能概述

- ✅ **批量处理**：从列表文件读取多个URL进行批量处理
- ✅ **并发控制**：使用线程池并发执行，最大并发数为10
- ✅ **重试机制**：支持最多3次重试，采用指数退避策略（最大30秒间隔）
- ✅ **限流控制**：每秒最多5个请求，使用滑动窗口算法精确控制
- ✅ **超时控制**：双层超时保护（内部60秒 + 外部70秒强制超时）
- ✅ **线程安全**：使用独立锁机制确保并发安全
- ✅ **客户端复用**：每个Worker线程独立维护VOD客户端，避免重复创建
- ✅ **智能重试**：区分可重试错误和不可重试错误
- ✅ **进度显示**：实时显示任务进度、成功率和重试次数
- ✅ **详细日志**：自动生成带时间戳的日志文件
- ✅ **结果保存**：完整的JSON格式结果报告，包含详细统计分析
- ✅ **参数验证**：严格的配置参数验证和URL格式检查

## 文件结构

```
pull_upload_list/
├── batch_pull_upload_api.sh   # 安装云 API Python SDK的脚本
├── batch_pull_upload.py      # 主脚本文件（核心实现）
├── config.json              # 配置文件（腾讯云API密钥）
├── test_urls.txt           # URL列表示例文件
└── README.md               # 说明文档（本文件）
```

**生成的文件：**
- `pull_upload_YYYYMMDD_HHMMSS.log` - 执行日志文件
- `pull_upload_result_YYYYMMDD_HHMMSS.json` - 详细结果报告

## 使用方法

### 0. 安装云api-python-sdk
进入到脚本所在目录，执行如下命令

```bash
bash batch_pull_upload_api.sh
```

### 1. 准备URL列表文件

创建一个文本文件，支持多种格式，每行一个任务，支持注释行：

**格式说明：**
- **列分隔符**：使用英文逗号 `,` 分隔各列
- **第1列（必需）**：URL地址，必须以 `http://` 或 `https://` 开头
- **第2列（可选）**：MediaName 媒体名称，支持中文，留空表示使用默认名称
- **第3列（可选）**：ClassId 分类ID，数字格式，留空表示使用默认分类

**URL格式要求：**
- 必须以 `http://` 或 `https://` 开头
- 支持所有有效的HTTP/HTTPS URL格式
- 每行只能包含一个URL任务
- 文件编码必须为UTF-8（支持中文媒体名称）

#### 🎯 格式1：纯URL格式
```
# 这是注释行（以#开头，会被忽略）
https://example.com/video1.mp4
https://example.com/video2.mp4

# 空行会被自动跳过
https://example.com/video3.mp4
```

#### 🎯 格式2：URL + 媒体名称格式
```
https://example.com/video1.mp4,我的视频1,
https://example.com/video2.mp4,测试视频,
```

#### 🎯 格式3：URL + 分类ID格式
```
https://example.com/video1.mp4,,1001
https://example.com/video2.mp4,,1002
```

#### 🎯 格式4：完整三列格式（推荐）
```
https://example.com/video1.mp4,我的视频1,1001
https://example.com/video2.mp4,测试视频,1002
```

#### 🎯 格式5：混合使用（完全支持）
```
# 纯URL
https://example.com/video1.mp4

# URL + 名称
https://example.com/video2.mp4,视频名称,

# URL + 分类ID
https://example.com/video3.mp4,,1003

# 完整格式
https://example.com/video4.mp4,完整视频,1004
```

### 2.准备配置文件
脚本会自动读取脚本所在目录的配置文件：`config.json`

**配置文件示例：**
```json
{
    "secret_id": "your_secret_id_here",
    "secret_key": "your_secret_key_here", 
    "region": "ap-guangzhou",
    "subappid": 0,
    "tasks_priority": 0,
    "procedure": ""
}
```

### 3. 运行脚本

```bash
python3 batch_pull_upload.py your_url_list.txt
```

### 4. 查看结果

**实时监控：**
- 控制台实时显示进度条和任务状态
- 自动记录成功/失败任务数量
- 显示重试次数和执行时间

**结果文件：**
- `pull_upload_YYYYMMDD_HHMMSS.log` - 详细执行日志
- `pull_upload_result_YYYYMMDD_HHMMSS.json` - 完整结果报告

## 配置说明

**配置验证机制：**
- ✅ 自动检查配置文件是否存在
- ✅ 验证JSON格式是否正确
- ✅ 验证所有必需字段是否存在且非空
- ✅ 验证字段类型（subappid和tasks_priority必须是数字）
- ✅ 提供详细的错误提示信息

**必需配置项：**
- `secret_id` - 腾讯云API密钥ID
- `secret_key` - 腾讯云API密钥Key
- `region` - 云点播服务区域（如：ap-guangzhou）
- `subappid` - 子应用ID（默认为"0"）

**可选配置项：**
- `tasks_priority` - 任务优先级（可选，默认为0）
  - 数值越大优先级越高
  - 范围：通常为-10到10，具体请参考腾讯云文档
  - 示例：`"tasks_priority": 5` 表示高优先级

- `procedure` - 任务流模板名称（可选，默认为空字符串）
  - 用于指定上传后的处理流程
  - 需要在腾讯云控制台预先创建任务流模板
  - 示例：`"procedure": "MyCustomProcess"` 指定自定义处理流程

## 核心参数配置

### 内置默认参数
```python
# 并发控制
max_workers = 10                    # 最大并发线程数

# 限流设置
max_requests_per_second = 5         # 每秒最大请求数

# 重试设置
max_retries = 3                     # 最大重试次数
INTERNAL_TIMEOUT = 60               # 内部超时时间（秒）
EXTERNAL_TIMEOUT = 70               # 外部强制超时时间（秒）
```

### 自定义参数
如需调整参数，可修改脚本中的对应常量或类初始化参数：

```python
# 调整并发数
uploader = BatchPullUploader(max_workers=15)

# 调整限流策略
rate_limiter = RateLimiter(max_requests_per_second=10)

# 调整重试策略
worker = PullUploadWorker(max_retries=5)
```

## 输出格式

### 控制台实时输出
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

### JSON结果文件格式
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
      "media_name": "我的视频1",
      "class_id": 1001,
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
      "error": "INVALID_URL: Invalid URL format",
      "error_code": "INVALID_URL",
      "retry_attempts": 2,
      "final_failure": true,
      "total_duration": 45.2
    }
  ]
}
```

## 错误处理机制

脚本采用多层错误处理策略，确保在各种异常情况下的稳定运行：

### 1. 配置层错误处理
- **文件不存在**：自动检测并提示配置文件路径
- **JSON格式错误**：详细的格式验证和错误定位
- **参数缺失**：检查所有必需字段，提供具体缺失信息
- **参数类型错误**：验证字段类型（如subappid必须为数字）

### 2. URL解析层错误处理
- **文件权限**：处理文件读取权限问题
- **编码问题**：强制UTF-8编码，处理中文URL
- **URL格式验证**：严格的URL格式检查
- **空行和注释**：自动跳过空行和注释行

### 3. API调用层错误处理
- **TencentCloudSDK异常**：捕获所有SDK异常，提取详细错误信息
- **网络超时**：双层超时保护（内部检查+外部强制）
- **参数验证**：安全的参数处理，防止参数错误导致的崩溃
- **系统异常**：捕获所有未预期异常，确保程序不崩溃

### 4. 重试机制错误处理
- **智能重试判断**：区分可重试错误和不可重试错误
  - ✅ 可重试：网络错误、API限流、临时服务异常
  - ❌ 不可重试：URL格式错误、参数错误、认证失败
- **指数退避**：重试间隔递增（2^attempt，最大30秒）
- **总时间控制**：防止无限重试，60秒总超时

### 5. 结果保存错误处理
- **保存重试**：文件保存失败时自动重试3次
- **内存保护**：成功保存后才清理内存数据
- **用户提示**：提供手动保存建议

### 错误代码说明
| 错误代码 | 说明 | 是否重试 |
|---------|------|---------|
| `INVALID_URL` | URL格式错误 | ❌ 不重试 |
| `PARAM_ERROR` | 参数处理错误 | ❌ 不重试 |
| `INTERNAL_TIMEOUT` | 内部超时（60秒） | ✅ 重试 |
| `THREAD_POOL_TIMEOUT` | 外部强制超时（70秒） | ❌ 不重试 |
| `SYSTEM_ERROR` | 系统异常 | ✅ 重试 |
| `TencentCloudSDK异常` | 腾讯云API错误 | 根据具体错误判断 |

## 使用注意事项

### 🔑 权限要求
- 确保腾讯云API密钥具有云点播服务的读写权限
- 建议使用子应用ID进行资源隔离（`subappid`配置）
- 监控API配额使用，避免超出调用限制

### 📊 性能建议
- **批量大小**：建议单次处理不超过1000个URL
- **并发数调整**：根据服务器性能调整`max_workers`（5-20为宜）
- **限流设置**：根据API配额调整`max_requests_per_second`
- **网络环境**：稳定网络环境下运行效果最佳

### 🛡️ 安全注意事项
- 配置文件包含敏感信息，请妥善保管
- 建议使用临时密钥或最小权限原则
- 不要在代码中硬编码密钥信息
- 定期轮换API密钥

### 📝 最佳实践
1. **测试先行**：先用少量URL（1-5个）测试配置正确性
2. **监控日志**：关注日志文件中的错误和警告信息
3. **结果验证**：检查JSON结果文件中的成功率统计
4. **渐进处理**：大批量任务建议分批次处理
5. **异常处理**：网络不稳定时适当增加重试次数

### ⚠️ 常见问题
- **URL格式错误**：确保URL以http://或https://开头
- **文件编码**：URL列表文件必须使用UTF-8编码
- **权限错误**：检查config.json文件读取权限
- **超时问题**：大文件拉取可能需要调整超时时间

---

