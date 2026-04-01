from adodbapi.examples.db_table_names import provider
from sqlalchemy.testing import skip

from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult,MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

from astrbot.core.agent.run_context import ContextWrapper
from astrbot.core.agent.tool import FunctionTool, ToolExecResult
from astrbot.core.astr_agent_context import AstrAgentContext
import httpx
import atoma
from astrbot.api import AstrBotConfig
from bs4 import BeautifulSoup

@register("RssTextNews", "NaSAeL", "Rss新闻处理", "1.0.0")
class GetTextNews(Star):
    def __init__(self,  context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.context = context
        self._url_list = config['rss_urls']
        self._num = int(config['max_news_count'])
        self._target_groups = config['target_groups']
        self._schedule_cron = config['schedule_cron']
        self._llm_request_umo = config['llm_request_umo']
        self._enable_auto_send = config.get('enable_auto_send', False)

    #=============新闻处理=============
    async def get_pure_text(self, html_str):
        if not html_str:
            return ""
        try:
            soup = BeautifulSoup(html_str, "html.parser")
            return soup.get_text(separator=" ", strip=True)
        except Exception as e:
            logger.error(f"[RssTextNews] HTML 解析失败: {e}")
            return html_str

    async def get_news_from_rss(self, _url_list: list, _num: int):
        _news = ""
        async with httpx.AsyncClient() as client:
            for _url in _url_list:
                try:
                    _response = client.get(_url)
                    feed = atoma.parse_rss_bytes(_response.content)
                    for item in feed.items[:_num]:
                        _news += f"标题: {item.title}\n"
                        raw_html = item.description
                        pure_text = await self.get_pure_text(raw_html)
                        _news += f"正文摘要: {pure_text}\n"
                except Exception as e:
                    logger.error(f"[RssTextNews] 访问源 "+_news+f" 时发送错误：{e}")
        return _news

    # =============llm新闻总结=============
    async def get_skill_content(self):
        #拿到目录
        plugin_data_path = get_astrbot_data_path() / "plugins" / self.name
        #拿到技能文件path
        skill_file_path = plugin_data_path / "SKILL.md"
        
        default_skill = "#每日新闻播报总结：你需要对后文提到的新闻内容选取15个重磅内容进行总结，按照「[简洁标题]：[正文50字]」的格式进行生成，仅输出播报文本，不输出任何其他内容。"
        
        try:
            with open(skill_file_path, "r", encoding="utf-8") as f:
                skill_content = f.read()
            return skill_content if skill_content else default_skill
        except Exception as e:
            logger.error(f"[RssTextNews] 打开SKILL.md时发送错误：{e} 。将给AI返回默认SKILL内容")
            return default_skill

    async def get_llm_respond(self,_llm_request_umo:str):
        #拿到对话provider_id
        provider_id = await self.context.get_current_chat_provider_id(umo=_llm_request_umo)
        #构建提示词：
        skill_content = await self.get_skill_content()
        news_content = await self.get_news_from_rss(self._url_list, self._num)

        pre_prompt = "请按照这份SKILL的要求：" + skill_content + "\n\n对该新闻内容进行总结（仅输出播报文本）：" + news_content
        #发出请求
        try:
            llm_resp = await self.context.llm_generate(
                chat_provider_id=provider_id,  # 聊天模型 ID
                prompt=pre_prompt,
            )
            logger.info("[RssTextNews] LLM响应成功")
        except Exception as e:
            logger.error("[RssTextNews] 在获取LLM响应时发生错误:", e)
            llm_resp = "[RssTextNews] 获取LLM响应失败，请检查日志以获取详细信息。"
        return llm_resp

    # ================发送信息=================
    async def send_news_to_groups(self, _target_groups: list, _content: MessageChain):
        for group_id in _target_groups:
            try:
                await self.context.send_message(group_id, _content)
                logger.info("[RssTextNews] "+group_id+" 推送新闻成功！")
            except Exception as e:
                logger.error(f"[RssTextNews] 向群 {group_id} 发送消息时发生错误:", e)

    # ================定时任务==================
    async def register_schedule(self):
        try:
            # 检查是否启用自动发送
            if not self._enable_auto_send:
                logger.info("[RssTextNews] 自动发送未启用，跳过定时任务注册")
                return
            
            # 使用 cron_manager 注册定时任务
            await self.context.cron_manager.add_basic_job(
                name="daily_news_broadcast",
                cron_expression=self._schedule_cron,
                handler=self.run_daily_news, #此处让每日任务调用主方法
                description="每日新闻播报定时任务",
                timezone="Asia/Shanghai",
                enabled=True,
                persistent=False,
            )
            logger.info("[RssTextNews] 定时任务注册成功！")
        except Exception as e:
            logger.error("[RssTextNews] 定时任务注册失败:", e)

    @filter.llm_tool(name="start_daily_news")
    async def run_daily_news(self):
        """执行每日新闻播报任务"""
        try:
            logger.info("[RssTextNews] 开始执行每日新闻播报任务")
            
            # 获取 LLM 总结的新闻内容
            llm_response = await self.get_llm_respond(self._llm_request_umo)
            
            # 构建消息链
            message_chain = MessageChain().message(llm_response)
            
            # 发送到目标群组
            await self.send_news_to_groups(self._target_groups, message_chain)

            # 注册第二天的任务
            await self.register_schedule()
            
            logger.info("[RssTextNews] 每日新闻播报任务执行完成")
        except Exception as e:
            logger.error("[RssTextNews] 每日新闻播报任务执行失败:", e)
