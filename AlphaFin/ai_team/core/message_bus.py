"""
智能体间消息总线 - 线程安全
复用 progress_service.py 的 Lock + dict 模式
"""
import threading
import time
import uuid

try:
    from queue import Queue
except ImportError:
    from Queue import Queue


class MessageBus:
    """线程安全的智能体间消息队列与广播日志"""

    def __init__(self):
        self._queues = {}        # {agent_id: Queue}
        self._log = []           # 所有消息的有序列表（供前端 SSE）
        self._lock = threading.Lock()
        self._seq = 0            # 全局消息序号
        self._session_results = {}  # {session_id: {agent_id: result_content}}

    def register_agent(self, agent_id):
        """注册智能体的消息队列"""
        with self._lock:
            if agent_id not in self._queues:
                self._queues[agent_id] = Queue()

    def send(self, from_agent, to_agent, msg_type, content, metadata=None):
        """
        发送定向消息。

        msg_type: 'task' | 'report' | 'question' | 'review' | 'alert' | 'consensus' | 'status'
        """
        msg = self._make_msg(from_agent, to_agent, msg_type, content, metadata)
        if to_agent in self._queues:
            self._queues[to_agent].put(msg)
        return msg

    def broadcast(self, from_agent, msg_type, content, metadata=None):
        """广播消息给所有其他智能体"""
        msg = self._make_msg(from_agent, 'all', msg_type, content, metadata)
        for agent_id, q in self._queues.items():
            if agent_id != from_agent:
                q.put(msg)
        return msg

    def receive(self, agent_id, timeout=1.0):
        """
        接收一条消息（阻塞，带超时）。
        返回 None 如果超时无消息。
        """
        q = self._queues.get(agent_id)
        if not q:
            return None
        try:
            return q.get(timeout=timeout)
        except Exception:
            return None

    def receive_all(self, agent_id):
        """接收所有待处理消息（非阻塞）"""
        q = self._queues.get(agent_id)
        if not q:
            return []
        messages = []
        while not q.empty():
            try:
                messages.append(q.get_nowait())
            except Exception:
                break
        return messages

    def get_recent_activity(self, limit=50):
        """获取最近的活动日志（供前端显示）"""
        with self._lock:
            return list(self._log[-limit:])

    def get_activity_since(self, since_seq):
        """获取指定序号之后的所有消息（供 SSE 增量推送）"""
        with self._lock:
            return [m for m in self._log if m['seq'] > since_seq]

    def post_activity(self, from_agent, activity_type, content, metadata=None):
        """
        发布活动日志（不进入任何智能体的队列，仅记录到日志供前端显示）。
        用于记录工具调用、状态变更等非消息事件。

        activity_type: 'tool_call' | 'thinking' | 'speaking' | 'idle' | 'error'
        """
        msg = self._make_msg(from_agent, None, activity_type, content, metadata)
        return msg

    def _make_msg(self, from_agent, to_agent, msg_type, content, metadata=None):
        """构造消息并记录到日志"""
        with self._lock:
            self._seq += 1
            msg = {
                'id': str(uuid.uuid4()),
                'seq': self._seq,
                'from': from_agent,
                'to': to_agent,
                'type': msg_type,
                'content': content,
                'metadata': metadata or {},
                'timestamp': time.time(),
            }
            self._log.append(msg)
            # 防止日志无限增长
            if len(self._log) > 5000:
                self._log = self._log[-3000:]
            return msg

    def save_result(self, session_id, agent_id, content):
        """保存智能体的阶段性结果（供后续阶段使用）"""
        with self._lock:
            if session_id not in self._session_results:
                self._session_results[session_id] = {}
            self._session_results[session_id][agent_id] = content

    def get_session_results(self, session_id):
        """获取某个session的所有智能体结果"""
        with self._lock:
            return dict(self._session_results.get(session_id, {}))

    def clear_session(self, session_id):
        """清理session数据"""
        with self._lock:
            self._session_results.pop(session_id, None)

    def clear(self):
        """清空所有队列和日志"""
        with self._lock:
            for q in self._queues.values():
                while not q.empty():
                    try:
                        q.get_nowait()
                    except Exception:
                        break
            self._log.clear()
            self._seq = 0


# 全局单例
bus = MessageBus()
