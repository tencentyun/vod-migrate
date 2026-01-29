# -*- coding:utf-8 -*-

"""云点播批量拉取上传脚本

本工具支持批量并发拉取上传媒体文件：
1. 从列表文件读取待拉取的媒体URL、MediaName、ClassId
2. 使用线程池并发处理，最大并发数为10
3. 实现重试、超时、限流控制机制
4. 提供详细的日志记录和进度显示
"""

from urllib.parse import urlparse

import json
import sys
import os
import time
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import tencentcloud.common.credential
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.vod.v20180717 import vod_client, models

# 配置日志格式
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# 超时配置
INTERNAL_TIMEOUT = 60  # 内部重试检查超时时间（秒）和 腾讯云SDK单次接口请求默认超时时间保持一致
EXTERNAL_TIMEOUT = 70  # 线程池强制超时时间（秒），应该略大于内部超时

class PullUploadConfig:
    """配置管理类"""
    def __init__(self, config_file=None):
        if config_file is None:
            # 获取脚本所在目录
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_file = os.path.join(script_dir, "config.json")
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self):
        """加载配置文件"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # 验证必要的配置项（存在且不为空）
            required_keys = ["secret_id", "secret_key", "region", "subappid"]
            invalid_keys = [
                key for key in required_keys 
                if key not in config or not config.get(key) or not str(config.get(key)).strip()
            ]
            if invalid_keys:
                logging.error(f"Configuration items missing or empty: {', '.join(invalid_keys)}")
                sys.exit(1)

            return config
            
        except FileNotFoundError:
            logging.error(f"Configuration file {self.config_file} does not exist")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logging.error(f"Invalid configuration file format - {e}")
            sys.exit(1)


class RateLimiter:
    """简单限流控制类 - 计数器+sleep"""
    def __init__(self, max_requests_per_second=5):
        self.max_requests = max_requests_per_second
        self.request_count = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        
    def acquire(self):
        """获取请求许可"""
        with self.lock:
            current_time = time.time()
            
            # 检查是否需要重置时间窗口
            if current_time - self.start_time >= 1.0:
                self.request_count = 0
                self.start_time = current_time
            
            # 如果当前窗口已满，等待到下个时间窗口
            if self.request_count >= self.max_requests:
                # 计算需要等待的时间，加上小缓冲避免时间精度问题
                wait_time = 1.0 - (current_time - self.start_time) + 0.001
                time.sleep(max(0, wait_time))
                
                # 重置计数器和时间戳
                self.request_count = 0
                self.start_time = time.time()
            
            # 增加计数
            self.request_count += 1


class PullUploadWorker:
    """拉取上传工作线程类"""
    def __init__(self, config, rate_limiter, max_retries=3):
        self.config = config
        self.rate_limiter = rate_limiter
        self.max_retries = max_retries
        # 初始化时创建客户端，避免每次调用重复创建
        self.client = self._create_client()
        
    def _create_client(self):
        """创建VOD客户端"""
        try:
            cred = tencentcloud.common.credential.Credential(
                self.config["secret_id"], 
                self.config["secret_key"]
            )
            client = vod_client.VodClient(cred, self.config["region"])
            return client
        except Exception as e:
            raise Exception(f"Failed to create VOD client: {e}")
    
    def _build_media_storage_path(self, url, media_storage_path=None):
        storage_path_config = self.config.get("custom_path", {})
        use_url_path = storage_path_config.get("use_url_path", False)
        prefix = storage_path_config.get("prefix", "")

        final_path = ""

        if use_url_path:
            # 优先使用 URL 中的 path
            parsed_url = urlparse(url)
            url_path = parsed_url.path
            if url_path:
                final_path = url_path
                logging.debug(f"use_url_path=true, using URL path: {url_path}")
        elif media_storage_path and media_storage_path.strip():
            # 其次使用用户提供的 MediaStoragePath（必须以 / 开头）
            if media_storage_path.strip().startswith('/'):
                final_path = media_storage_path.strip()

        if prefix and prefix.strip():
            prefix = prefix.strip()
            # 确保 prefix 以 / 开头，并且不以 / 结尾
            if not prefix.startswith('/'):
                prefix = '/' + prefix
            prefix = prefix.rstrip('/')

            if final_path:
                # 如果已有路径，拼接 prefix + path
                result = prefix + final_path
                logging.debug(f"Using combined path: prefix={prefix}, path={final_path}, final={result}")
            else:
                # 如果没有路径，只使用 prefix
                parsed_url = urlparse(url)
                filename = parsed_url.path.split('/')[-1]
                result = prefix + '/' + filename
                logging.debug(f"Using prefix only: {prefix}")
            return result
        elif final_path:
            # 如果没有 prefix 但有路径，使用路径
            return final_path

        # 没有路径需要设置
        return None

    def _pull_single_media(self, url, media_name=None, class_id=None, media_storage_path=None):
        """单次拉取上传操作"""
        # 验证URL格式
        if not url or not isinstance(url, str):
            return {
                "success": False,
                "url": str(url) if url else "<empty>",
                "error": "Invalid URL format",
                "error_code": "INVALID_URL"
            }
        # 请求参数
        params = {"MediaUrl": url}
        
        # 添加MediaName参数
        if media_name and media_name.strip():
            params["MediaName"] = media_name.strip()
        
        # 添加ClassId参数
        if class_id and class_id.strip():
            try:
                params["ClassId"] = int(class_id.strip())
            except ValueError:
                logging.warning(f"Invalid ClassId format: {class_id}, must be integer, skipping")

        # 安全地添加可选参数
        try:
            # 构建并设置 MediaStoragePath
            storage_path = self._build_media_storage_path(url, media_storage_path)
            if storage_path:
                params["MediaStoragePath"] = storage_path

            if "subappid" in self.config:
                subappid = self.config["subappid"]
                if isinstance(subappid, (int, str)) and str(subappid).isdigit():
                    params["SubAppId"] = int(subappid)
                else:
                    logging.warning(f"Invalid subappid format: {subappid}, skipping")
            
            # 添加TasksPriority参数
            if "tasks_priority" in self.config:
                priority = self.config["tasks_priority"]
                if isinstance(priority, (int, str)) and str(priority).isdigit():
                    params["TasksPriority"] = int(priority)
                else:
                    logging.warning(f"Invalid tasks_priority format: {priority}, must be integer, skipping")
            
            # 添加Procedure参数
            if "procedure" in self.config:
                procedure = self.config["procedure"]
                if procedure and procedure.strip():
                    params["Procedure"] = procedure.strip()
                
        except Exception as e:
            return {
                "success": False,
                "url": url,
                "error": f"Parameter processing error: {e}",
                "error_code": "PARAM_ERROR"
            }

        try:
            # 限流控制
            self.rate_limiter.acquire()
            
            # 使用已初始化的客户端，避免重复创建
            method = getattr(models, "PullUploadRequest")
            req = method()
            req.from_json_string(json.dumps(params))
            
            start_time = time.time()
            rsp = self.client.PullUpload(req)
            end_time = time.time()
            
            return {
                "success": True,
                "url": url,
                "response": rsp.to_json_string(),
                "duration": end_time - start_time,
                "task_id": rsp.TaskId if hasattr(rsp, 'TaskId') else ""
            }
            
        except TencentCloudSDKException as e:
            return {
                "success": False,
                "url": url,
                "error": str(e),
                "error_code": getattr(e, 'code', 'UNKNOWN')
            }
        except Exception as e:
            # 记录更详细的错误信息
            error_msg = str(e)
            error_type = type(e).__name__
            
            return {
                "success": False,
                "url": url,
                "error": f"{error_type}: {error_msg}",
                "error_code": "SYSTEM_ERROR"
            }
    
    def pull_with_retry(self, url, media_name=None, class_id=None, media_storage_path=None, external_timeout=INTERNAL_TIMEOUT):
        """带重试机制的拉取上传"""
        last_error = None
        total_start_time = time.time()
        
        for attempt in range(self.max_retries + 1):
            # 检查总执行时间是否过长（主动检查，提供更详细的错误信息）
            elapsed_time = time.time() - total_start_time
            if elapsed_time > external_timeout:
                return {
                    "success": False,
                    "url": url,
                    "error": f"Operation timeout after {external_timeout}s",
                    "error_code": "INTERNAL_TIMEOUT",
                    "retry_attempts": attempt,
                }
            
            if attempt > 0:
                # 重试等待时间：指数退避，最大30秒
                wait_time = min(2 ** attempt, 30)
                logging.info(f"[RETRY] Waiting {wait_time}s before retry {attempt}/{self.max_retries}")
                time.sleep(wait_time)

            result = self._pull_single_media(url, media_name, class_id, media_storage_path)
            
            if result["success"]:
                if attempt > 0:
                    result["retry_attempts"] = attempt
                    result["total_duration"] = time.time() - total_start_time
                return result
            else:
                last_error = result
                
                # 判断是否应该重试
                error_code = result.get("error_code", "")
                if error_code in ["INVALID_URL", "PARAM_ERROR"]:
                    # 参数错误不应该重试
                    break
                    
                logging.warning(f"[RETRY {attempt}/{self.max_retries}] {url[:50]}{'...' if len(url) > 50 else ''} - {result.get('error', 'Unknown error')}")
        
        return {
            **last_error,
            "retry_attempts": self.max_retries,
            "final_failure": True,
            "total_duration": time.time() - total_start_time
        }


class BatchPullUploader:
    """批量拉取上传管理类"""
    def __init__(self, config_file=None, max_workers=10, log_level=logging.INFO):
        self.config = PullUploadConfig(config_file)
        self.rate_limiter = RateLimiter(max_requests_per_second=5)
        self.worker = PullUploadWorker(self.config.config, self.rate_limiter)
        self.max_workers = max_workers
        self.total_tasks = 0
        self.completed_tasks = 0
        self.success_tasks = 0
        self.failed_tasks = 0
        self.results = []
        self.lock = threading.Lock()
        self.start_time = None
        self.end_time = None
        self.logger = self._setup_logger(log_level)
        
    def _setup_logger(self, log_level):
        """设置日志记录器"""
        # 创建日志文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"pull_upload_{timestamp}.log"
        
        # 配置日志格式
        formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
        
        # 创建文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # 配置根日志记录器
        logger = logging.getLogger('batch_pull_upload')
        logger.setLevel(log_level)
        logger.handlers.clear()  # 清除已有处理器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.propagate = False
        logger.info(f"Log file created: {log_file}")
        return logger
        
    def _parse_url_list(self, url_list_file):
        """解析URL列表文件，支持四列格式：URL,MediaName,ClassId,MediaStoragePath"""
        if not os.path.exists(url_list_file):
            self.logger.error(f"URL list file {url_list_file} does not exist")
            sys.exit(1)

        tasks = []
        try:
            with open(url_list_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    # 跳过空行和注释行
                    if not line or line.startswith('#'):
                        continue

                    # 解析四列格式，支持逗号分隔
                    parts = [part.strip() for part in line.split(',')]
                    
                    if len(parts) < 1:
                        continue
                    
                    url = parts[0]
                    media_name = parts[1] if len(parts) > 1 and parts[1] else None
                    class_id = parts[2] if len(parts) > 2 and parts[2] else None
                    media_storage_path = parts[3] if len(parts) > 3 and parts[3] else None

                    # 基本URL格式验证
                    if not (url.startswith('http://') or url.startswith('https://')):
                        self.logger.warning(f"Line {line_num} - Invalid URL format (must start with http:// or https://): {url}")
                        continue

                    if media_storage_path and media_storage_path.strip() and not media_storage_path.strip().startswith('/'):
                        self.logger.warning(f"Line {line_num} - Invalid MediaStoragePath format: {media_storage_path}, must start with '/'")
                        continue

                    tasks.append((line_num, url, media_name, class_id, media_storage_path))

                    # 记录解析信息
                    self.logger.debug(f"Line {line_num} parsed: {line}")
                        
            if not tasks:
                self.logger.error("No valid tasks found in the URL list file")
                sys.exit(1)
            
            self.logger.info(f"Successfully parsed {len(tasks)} tasks from {url_list_file}")
                
        except UnicodeDecodeError:
            self.logger.error(f"Failed to read {url_list_file} - file encoding must be UTF-8")
            sys.exit(1)
        except PermissionError:
            self.logger.error(f"No permission to read {url_list_file}")
            sys.exit(1)
        except Exception as e:
            self.logger.error(f"Failed to read URL list file - {type(e).__name__}: {e}")
            sys.exit(1)
            
        return tasks
    
    def _update_progress(self, result):
        """更新进度"""
        with self.lock:
            self.completed_tasks += 1
            if result["success"]:
                self.success_tasks += 1
            else:
                self.failed_tasks += 1
            self.results.append(result)
            
            # 显示进度
            progress = (self.completed_tasks / self.total_tasks) * 100
            status = "SUCCESS" if result["success"] else "FAILED"
            retry_info = f" (retry {result.get('retry_attempts', 0)} times)" if result.get('retry_attempts', 0) > 0 else ""
            
            self.logger.info(f"[{self.completed_tasks}/{self.total_tasks}] {progress:.1f}% | {status} | "
                           f"{result['url'][:50]}{'...' if len(result['url']) > 50 else ''}{retry_info}")
    
    def _print_summary(self):
        """打印执行摘要"""
        self.end_time = time.time()
        total_duration = self.end_time - self.start_time if self.start_time else 0
        
        self.logger.info("="*80)
        self.logger.info("Batch pull upload completed")
        self.logger.info("="*80)
        self.logger.info(f"Total tasks: {self.total_tasks}")
        self.logger.info(f"Successful tasks: {self.success_tasks}")
        self.logger.info(f"Failed tasks: {self.failed_tasks}")
        self.logger.info(f"Total execution time: {total_duration:.2f}s")
        
        # 避免除零错误
        if self.total_tasks > 0:
            success_rate = (self.success_tasks/self.total_tasks)*100
            throughput = self.total_tasks / total_duration if total_duration > 0 else 0
            self.logger.info(f"Success rate: {success_rate:.2f}%")
            self.logger.info(f"Throughput: {throughput:.2f} tasks/second")
        else:
            self.logger.info("Success rate: N/A (no tasks)")
            self.logger.info("Throughput: N/A (no tasks)")
        
        # 统计错误类型
        error_counts = {}
        total_retries = 0
        total_duration = 0
        
        for result in self.results:
            if not result["success"]:
                error_code = result.get("error_code", "UNKNOWN")
                error_counts[error_code] = error_counts.get(error_code, 0) + 1
            
            retry_count = result.get("retry_attempts", 0)
            if retry_count:
                total_retries += retry_count
            
            duration = result.get("total_duration", 0)
            if duration:
                total_duration += duration
        
        if error_counts:
            self.logger.info("Error breakdown:")
            for error_code, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True):
                self.logger.info(f"  {error_code}: {count}")
        
        if total_retries > 0:
            self.logger.info(f"Total retry attempts: {total_retries}")
        
        if total_duration > 0:
            avg_duration = total_duration / len(self.results)
            self.logger.info(f"Average task duration: {avg_duration:.2f}s")
        
        # 保存详细结果到文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file = f"pull_upload_result_{timestamp}.json"
        
        try:
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "summary": {
                        "total": self.total_tasks,
                        "success": self.success_tasks,
                        "failed": self.failed_tasks,
                        "success_rate": (self.success_tasks/self.total_tasks)*100 if self.total_tasks > 0 else 0,
                        "error_breakdown": error_counts,
                        "total_retries": total_retries,
                        "average_duration": total_duration / len(self.results) if self.results else 0
                    },
                    "results": self.results
                }, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Detailed results saved to: {result_file}")

        except Exception as e:
            self.logger.warning(f"Failed to save result file - {e}")

        self.results.clear()
    
    def run(self, url_list_file):
        """执行批量拉取上传"""
        self.start_time = time.time()
        
        self.logger.info(f"Starting batch pull upload, max concurrent workers: {self.max_workers}")
        self.logger.info(f"Rate limiting: max 5 requests per second")
        self.logger.info(f"Retry setting: max 3 retries with exponential backoff")
        
        # 解析URL列表
        urls = self._parse_url_list(url_list_file)
        self.total_tasks = len(urls)
        
        self.logger.info(f"Loaded {self.total_tasks} tasks from {url_list_file}")
        self.logger.info("-" * 80)
        
        # 使用线程池并发执行
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_task = {
                executor.submit(self.worker.pull_with_retry, url, media_name, class_id, media_storage_path): (line_num, url, media_name, class_id, media_storage_path)
                for line_num, url, media_name, class_id, media_storage_path in urls
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_task):
                line_num, url, media_name, class_id, media_storage_path = future_to_task[future]
                try:
                    # 设置单个任务的总超时时间（线程池强制超时，作为最后保障）
                    # 注意：这个超时应该略大于内部超时，给内部检查留出时间
                    result = future.result(timeout=EXTERNAL_TIMEOUT)  # 比内部超时多10秒，作为最后保障
                    result["line_num"] = line_num
                    result["media_name"] = media_name
                    result["class_id"] = class_id
                    self._update_progress(result)
                except TimeoutError:
                    # 线程池强制超时，说明任务可能卡死
                    error_result = {
                        "line_num": line_num,
                        "success": False,
                        "url": url,
                        "media_name": media_name,
                        "class_id": class_id,
                        "error": f"Task execution timeout ({EXTERNAL_TIMEOUT}s)",
                        "error_code": "THREAD_POOL_TIMEOUT",
                        "timeout_type": "external"
                    }
                    self._update_progress(error_result)
                except Exception as e:
                    error_result = {
                        "line_num": line_num,
                        "success": False,
                        "url": url,
                        "media_name": media_name,
                        "class_id": class_id,
                        "error": f"Task execution error: {type(e).__name__}: {str(e)}",
                        "error_code": "TASK_EXECUTION_ERROR"
                    }
                    self._update_progress(error_result)
        
        # 打印执行摘要
        self._print_summary()


def usage():
    """脚本用法"""
    print("Usage: python3 batch_pull_upload.py {url_list_file}")
    print("")
    print("url_list_file format example:")
    print("https://example.com/video1.mp4,我的视频1,1001,/custom/path/video1.mp4")
    print("https://example.com/video2.mp4,我的视频2,")
    print("# This is a comment line and will be ignored")
    print("https://example.com/video3.mp4,,1002")
    print("")
    print("Format: URL,MediaName,ClassId,MediaStoragePath")
    print("- URL: 必填，媒体文件URL")
    print("- MediaName: 可选，媒体文件名称")
    print("- ClassId: 可选，分类ID，用于对媒体进行分类管理，可创建分类后获得分类 ID。")
    print("- MediaStoragePath: 可选，媒体存储路径，以/开头，只有FileID + Path 模式的子应用可以指定存储路径。")
    print("")
    print("NOTE:")
    print("- Columns are separated by commas. Empty values are allowed for optional fields.")
    print("- 如果配置里指定 storage_path.use_url_path = true 时，则保持原路径，以url后的path为存储路径。")
    print("- 如果配置里指定 storage_path.prefix，则所有媒体都会加上该前缀。")
    print("- 路径组合优先级：use_url_path=true 时使用 url_path，否则使用 MediaStoragePath，最后拼接 prefix。")


def main():
    """主函数"""
    if len(sys.argv) != 2:
        usage()
        sys.exit(1)
    
    url_list_file = sys.argv[1]
    
    try:
        uploader = BatchPullUploader()
        uploader.run(url_list_file)
    except KeyboardInterrupt:
        logging.warning("Operation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Program execution error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()