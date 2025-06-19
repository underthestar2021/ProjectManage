import json
import math
import re
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from uuid import uuid4

import requests
import streamlit as st
import pandas as pd
from deepdiff import DeepDiff

from config import CONFIG
from database.sqlite import SQLiteOp
from util import init_session_state, get_env_pg, init_pg, get_env_pg_user_info, get_user_id, get_pg, get_sqlite, \
    get_fuse_pg


def get_folder_info_by_sql(env, user_id, with_backups=False):
    current_pg = get_env_pg(env)
    sql = "SELECT name, id FROM folder where user_id=%s"
    if with_backups:
        sql += " and (RIGHT(name, 1)='*' or name='备份')"
    else:
        sql += " and RIGHT(name, 1)='*'"
    result = current_pg.execute_query(sql, (user_id,))
    current_pg.commit()
    return result


def get_folder_info_by_api(langflow_token, base_url):
    url = f"{base_url}/api/v1/projects/"
    response = requests.get(url, headers={"Authorization": f'Bearer {langflow_token}'})
    dir_dict = {i['name']: i['id'] for i in response.json()}
    return dir_dict


@st.cache_data(ttl=300)
def login_langflow(username, password, url):
    payload = f'username={username}&password={password}'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }
    response = requests.request("POST", url + "/api/v1/login", headers=headers,
                                data=payload)
    return response.json()['access_token']


def create_folder_by_api(langflow_token, folder_name, base_url):
    data = {"name": folder_name, "description": "", "flows_list": [], "components_list": []}
    url = f"{base_url}/api/v1/projects/"
    response = requests.post(url, headers={"Authorization": f'Bearer {langflow_token}'}, json=data)
    if response.status_code == 201:
        st.success(f"创建{folder_name}目录成功")
        dir_id = response.json()["id"]
        return dir_id
    else:
        st.error(f"创建{folder_name}目录失败, {response.text}")


def create_new_flow_by_api(langflow_token, folder_id, one, base_url):
    new_data = {
        "name": one["name"],
        "description": one["description"],
        "data": one["data"],
        "endpoint_name": one["endpoint_name"],
        "gradient": one["gradient"],
        "is_component": one["is_component"],
        "tags": one["tags"],
        "mcp_enabled": True,
        "folder_id": folder_id,
        "icon": None,
    }
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {langflow_token}'
    }
    response = requests.post(f"{base_url}/api/v1/flows/", headers=headers, json=new_data)

    if response.status_code == 201:
        st.success(f"新建 {new_data['name']} 成功")
        return response.json()
    else:
        st.error(f"新建 {new_data['name']} 失败, {response.text}")
        return {}


@st.fragment
def get_data(search_term, user_id, folder_id, env, begin_time):
    sql = ("left join folder on flow.user_id=folder.user_id and flow.folder_id=folder.id WHERE flow.is_component=False "
           "AND folder.user_id='{}' and RIGHT(folder.name, 1)='*'").format(user_id)
    if folder_id:
        sql += f" AND flow.folder_id = '{folder_id}'"
    if begin_time:
        sql += f" AND flow.updated_at >= '{begin_time}'"
    if search_term:
        sql += f" AND flow.name ILIKE '%{search_term}%'"
    pg = get_env_pg(env)
    query = (("SELECT flow.name, folder.name, flow.description,flow.updated_at,flow.endpoint_name,"
              "flow.gradient,flow.is_component,flow.tags,flow.data FROM flow ") + sql +
             f" ORDER BY updated_at DESC LIMIT {st.session_state.deploy_page_size} "
             f"OFFSET {(st.session_state.deploy_page - 1) * st.session_state.deploy_page_size}")
    result = pg.execute_query(query)

    # 获取总记录数用于判断是否可以翻页
    count_query = "SELECT COUNT(*) FROM flow " + sql
    total_count = pg.execute_query(count_query)[0][0]

    pg.commit()
    st.session_state.deploy_data = (result, total_count)


def change_page_to_1():
    st.session_state.deploy_page = 1


class DeploymentPage:
    def __init__(self):
        # 初始化分页状态
        init_session_state('deploy_page', 1)
        init_session_state('deploy_data', None)
        init_session_state('deploy_page_size', 20)
        self.input_id = ("ChatInput", "Webhook", "TextInput")
        self.env_label = {"dev": "dev", "pro": "production", "test": "test", "beta": "stage"}
        self.fuse_table = "prompts"
        self.fuse_project_id = "cmbahi0nx00ympp085f23z64w"
        self.sqlite_op = get_sqlite()

    def get_version(self, env, current_user_id):
        new_version_pre = f"{datetime.now().strftime('%Y%m%d')}"
        with self.sqlite_op:
            last_version_info = self.sqlite_op.execute_query(
                "SELECT version FROM flow_history where environment=? and user_id=? order by created_at desc limit 1",
                (env, current_user_id))
            self.sqlite_op.commit()
            if not last_version_info:
                last_version_info = "20150618.0"
                self.sqlite_op.execute_update("insert into flow_history(name,version,environment, user_id,old_id) "
                                              "values (?,?,?,?,?)", ("", last_version_info, env, current_user_id, ""))
                return last_version_info, f"{new_version_pre}.1"
            else:
                last_version = last_version_info[0]['version']
                old_pre, old_suffix = last_version.split(".")
                if old_pre == new_version_pre:
                    new_version = f"{new_version_pre}.{int(old_suffix) + 1}"
                else:
                    new_version = f"{new_version_pre}.1"
                return last_version, new_version

    def deployment_page(self):
        init_pg()
        tabs = st.tabs(["备份管理", "flow上线", "刷新run_flow", "组件langfuse标签替换"])
        with tabs[0]:
            self.manage_backup()
        with tabs[1]:
            self.upload_flows()
        with tabs[2]:
            self.flush_run_flow()
        with tabs[3]:
            self.flush_label()

    @st.fragment
    def manage_backup(self):
        st.caption("本页面主要是用于管理备份相关的数据")
        selection = st.segmented_control(
            "1.请选择要管理的环境", (("开发", "dev"), ("测试", "test"), ("beta", "beta"), ("正式", "pro")),
            selection_mode="single", format_func=lambda x: x[0], key="manage_backup"
        )
        if selection is None:
            return
        current_user_info = get_env_pg_user_info(selection[1])
        current_user_id = get_user_id(selection[1], current_user_info.username)
        result = self.sqlite_op.execute_query(
            "SELECT version, group_concat(name, ' | ') as flow_op, created_at FROM flow_history where environment=? "
            "and user_id=? "
            "group by version order by created_at desc limit 10", (selection[1], current_user_id))
        self.sqlite_op.commit()
        if not result:
            st.warning("当前环境没有备份数据")
            return
        fuse_data = self.sqlite_op.execute_query(
            "select history as version, group_concat(name, ' | ') as fuse_op from fuse_history where label=? and operation!=? "
            "group by history order by created_at desc limit 10",
            (selection[1], 'init'))
        self.sqlite_op.commit()

        st.write("2.请从下面的表格里的最左边列勾选需要回退的版本数据")
        st.caption("下面的操作是将数据还原到选定版本操作之后的数据状态")
        order_data = pd.DataFrame(result)
        f_data = pd.DataFrame(fuse_data)
        merged_data = pd.merge(order_data, f_data, on='version', how='left').fillna("")
        newest_version = result[0]['version']
        merged_data.columns = ["版本", "操作的流名称", "创建时间", "操作的提示词"]
        data = st.dataframe(merged_data, on_select='rerun', selection_mode="single-row",
                            column_order=["版本", "操作的流名称", "操作的提示词", "创建时间", ])
        selected_data = data['selection']['rows']
        if not selected_data:
            return
        self.backup_version(merged_data.iloc[selected_data[0], :]['版本'], newest_version, selection[1],
                            current_user_id)

    @st.dialog("langflow版本回溯", width="large")
    def backup_version(self, version_info, newest_version, env, current_user_id):
        if newest_version == version_info:
            st.warning(f"当前版本数据已经是{newest_version}数据")
            return
        st.warning(
            f"您即将从版本:**:red[{newest_version}]** 回溯到版本:**:red[{version_info}]**，中间版本数据将会被删除，请谨慎操作")

        # flow
        history_flow_data = self.sqlite_op.execute_query(
            "select * from flow_history where environment=? and version > ? "
            "and version <= ? and user_id=? order by version desc",
            (env, version_info, newest_version, current_user_id))
        self.sqlite_op.commit()
        # 寻找回溯数据
        op_data = {}
        for data in history_flow_data:
            name = data['name']
            if not data['is_exist']:
                data['delete'] = True
            else:
                data['delete'] = False
            op_data[name] = data
        st.subheader("即将回溯的flow数据如下:")
        st.dataframe(pd.DataFrame(op_data.values()),
                     column_order=['name', 'delete', 'version', 'description', 'environment', 'created_at'])

        # fuse
        fuse_data = self.sqlite_op.execute_query(
            "select * from fuse_history where label=? and history > ? and history <= ?"
            "order by history desc",
            (env, version_info, newest_version))

        self.sqlite_op.commit()
        wait_update_fuse = {}
        for data in fuse_data:
            match data['operation']:
                case 'same':
                    continue
                case 'remove':  # 新比久少，还原旧的就得新增
                    wait_update_fuse[data['name']] = data
                case 'add':  # 新比久多，还原旧的就得删除
                    wait_update_fuse[data['name']] = data
                case 'change':  # 还原旧的就以旧的为准
                    wait_update_fuse[data['name']] = data

        st.subheader(f"为保证安全，请手动把{env}环境的fuse按照如下指令进行操作")
        for k,data in wait_update_fuse.items():
            match data['operation']:
                case 'remove':
                    st.warning(f'{env}环境新增提示词 :orange[{data["name"]}] 版本为⭐️:blue[{data["version"]}]')
                case 'add':
                    st.warning(f'{env}环境删除 :orange[{data["name"]}] 对应版本为⭐️:blue[{data["version"]}]的{env}标签')
                case 'change':
                    st.warning(f"{env}环境把提示词 :orange[{data["name"]}] 的{env}标签改到版本⭐️:blue[{data["version"]}]上面")
        if st.button("确认回溯后不可撤销", type="primary", key="submit8"):
            self.execute_backup(op_data, current_user_id, env, version_info)

    def execute_backup(self, op_data, current_user_id, env, version_info):
        pg = get_env_pg(env)
        folder_info = get_folder_info_by_sql(env, current_user_id)
        folder_dic = {f[1]: f[0] for f in folder_info}
        for flow_name, flow_data in op_data.items():
            if flow_data['delete']:
                try:
                    pg.execute_update("delete from flow where name=%s and user_id=%s", (flow_name, current_user_id))
                except Exception as e:
                    st.error(f"数据回溯失败，清除数据发生意外：{e}")
                    pg.rollback()
                    return
            else:
                folder_name = folder_dic.get(flow_data['folder_id'])
                if not folder_name:
                    # todo 创建被删除的目录
                    st.error(f"数据回溯失败，找不到id为{flow_data['folder_id']}的目录,请检查相关数据")
                    pg.rollback()
                    return
                pg_data = pg.execute_query("select * from flow where name=%s and user_id=%s",
                                           (flow_name, current_user_id),
                                           with_columns=True)
                if len(pg_data) != 1:
                    st.error(f"数据回溯失败，找不到名为{flow_name}的数据")
                    pg.rollback()
                    return
                if pg_data[0]['id'] != flow_data['old_id']:
                    st.error(f"{flow_data['name']}数据id不匹配，请检查相关数据")
                    pg.rollback()
                    return
                update_columns = []
                update_values = []
                for k, v in pg_data[0].items():
                    f_v = flow_data.get(k, 'null')
                    if v == f_v or k in ('id', 'endpoint_name'):
                        continue
                    update_columns.append(f"{k}=%s")

                    if isinstance(v, (list, dict)):
                        if not isinstance(f_v, str):
                            update_values.append(json.dumps(f_v))
                        else:
                            update_values.append(f_v)
                    elif f_v == 'null' or k in ('updated_at',):
                        update_values.append(f_v)
                    elif v is None:
                        update_values.append(f_v)
                    else:
                        source_type = type(v)
                        update_values.append(source_type(f_v))

                sql = f"UPDATE flow SET {', '.join(update_columns)} WHERE name=%s and user_id=%s"
                try:
                    pg.execute_update(sql, tuple(update_values) + (flow_name, current_user_id))
                except Exception as e:
                    st.error(f"{flow_data['name']}数据更新失败:{e}")
                    pg.rollback()
                    return
        try:
            self.sqlite_op.execute_update("delete from flow_history where version > ? and user_id=? and environment=?",
                                          (version_info, current_user_id, env), autocommit=False)
            self.sqlite_op.execute_update("delete from fuse_history where label=? and history > ?", (env, version_info))
        except Exception as e:
            st.error(f"数据回溯失败，清除备份数据发生意外：{e}")
            pg.rollback()
            self.sqlite_op.rollback()
            return
        pg.commit()
        self.sqlite_op.commit()
        try:
            with st.spinner("刷新run_flow数据中", show_time=True):
                wait_update, runflow_flow_map = self.generate_run_flow_data(pg, current_user_id)
                filter_update = {}
                set_op_data_key = set(op_data.keys())
                for wait_name in wait_update:
                    if wait_name in op_data:
                        filter_update[wait_name] = wait_update[wait_name]
                    elif runflow_flow_map[wait_name].intersection(set_op_data_key):
                        filter_update[wait_name] = wait_update[wait_name]
                self.execute_update_run_flow(filter_update, pg, current_user_id, with_button=False)
                if filter_update:
                    st.subheader("更新的run_flow数据如下:")
                    st.markdown(' '.join([f":orange-badge[:material/star: {one}]" for one in filter_update]))
        except Exception as e:
            st.error(f"刷新run_flow失败，请手动刷新run_flow数据：{e}")
        st.balloons()
        st.success("数据回溯操作成功")

    @st.fragment
    def upload_flows(self):
        st.caption("本页面主要是用于把flow从a环境同步到b环境，在上线的时候会自动形成操作版本备份，"
                   "在上线的时候会对提示词标签统一变成当前环境的标签，清除了fileSelect的默认值，把run_flow的options置为选中的数据，"
                   "操作完成后记得去刷新run_flow")
        select_env = st.segmented_control("1.请选择您的发布环境", (
            ("开发->测试", "dev", "test"),
            ("测试->beta", "test", "beta"),
            ("beta->生产", "beta", "pro"),
            ("生产->测试", "pro", "test"),
        ), format_func=lambda option: option[0], key="select_env", on_change=change_page_to_1)
        if not select_env:
            return

        current_env = select_env[1]
        future_env = select_env[2]
        current_user_info = get_env_pg_user_info(current_env)
        st.warning(
            f"您当前选择的环境是:**:red[{current_env}]**，"
            f"您要发布的环境是:**:red[{future_env}]**，"
            f"您正在使用的用户是:**:red[{current_user_info.username}]**"
        )

        st.subheader("数据展示")

        select_show_way = st.segmented_control("2.请选择数据展示方式", (
            ("按更新排序时间展示", "time"),
            ("按目录选择展示", "dir"),
        ), format_func=lambda option: option[0], key="select_display", on_change=change_page_to_1)
        if not select_show_way:
            return
        user_id = get_user_id(current_env, current_user_info.username)
        select_folder = None
        select_time = None
        if select_show_way[1] == "dir":
            # 选择文件夹
            select_folder = st.pills("**:red[选择文件夹]**", get_folder_info_by_sql(current_env, user_id),
                                     selection_mode="single",
                                     format_func=lambda option: option[0], on_change=change_page_to_1)
            if not select_folder:
                return
            select_folder = select_folder[1]
        else:
            select_time = st.pills("**:red[选择时间范围]**", (
                ("最近1天", 1), ("最近3天", 3), ("最近7天", 7), ("最近30天", 30)
            ), selection_mode="single", format_func=lambda option: option[0], on_change=change_page_to_1)
            if not select_time:
                return
            select_time = datetime.now() - timedelta(days=select_time[1])

        # 获取数据
        search_term = st.text_input(label="搜索流程名称", placeholder="请输入查询的流程名称", key="search_term",
                                    label_visibility="collapsed", on_change=change_page_to_1)
        get_data(search_term, user_id, select_folder, current_env, select_time)
        if st.session_state.deploy_data is None:
            return
        # 计算总页数
        total_pages = math.ceil(st.session_state.deploy_data[1] / st.session_state.deploy_page_size)

        df = pd.DataFrame(st.session_state.deploy_data[0],
                          columns=['name', 'folder_name', 'description', 'updated_at', 'endpoint_name', 'gradient',
                                   'is_component', 'tags', 'data'])
        # # 分页控制按钮
        col1, col2, col3 = st.columns([1, 5, 1])
        with col1:
            if st.button("上一页", disabled=st.session_state.deploy_page <= 1, use_container_width=True,
                         key="prev_page"):
                st.session_state.deploy_page -= 1
                if st.session_state.deploy_page < 1:
                    st.session_state.deploy_page = 1
                st.rerun()
        with col2:
            st.button(f"共有 {total_pages} 页，当前页: {st.session_state.deploy_page}", use_container_width=True,
                      disabled=True,
                      type="tertiary", key="current_page")
        with col3:
            if st.button("下一页", use_container_width=True, key="next_page",
                         disabled=st.session_state.deploy_page >= total_pages):
                st.session_state.deploy_page += 1
                st.rerun()
        self.show_data(df, future_env)

    @st.fragment
    def flush_run_flow(self):
        st.caption("本页面主要是用于刷新run_flow数据，会尝试更新数据，自动连线")
        selection = st.segmented_control(
            "1.请选择刷新环境", (("开发", "dev"), ("测试", "test"), ("beta", "beta"), ("正式", "pro")),
            selection_mode="single", format_func=lambda x: x[0], key="flush_rn_flow"
        )
        if selection is None:
            return
        current_env = selection[1]
        current_user_info = get_env_pg_user_info(current_env)
        st.warning(
            f"您当前选择的环境是:**:red[{current_env}]**，"
            f"您当前选择的用户是:**:red[{current_user_info.username}]**"
        )
        st.write("2.查询相关信息")
        if not st.button(
                f"查找用户 {current_user_info.username} 下的run_flow", type="primary", key="submit3"):
            return
        current_user_id = get_user_id(current_env, current_user_info.username)
        current_pg = get_env_pg(current_env)

        wait_update, _ = self.generate_run_flow_data(current_pg, current_user_id)
        self.execute_update_run_flow(wait_update, current_pg, current_user_id)

    def generate_run_flow_data(self, current_pg, current_user_id):
        my_bar = st.progress(0.0, text="开始查找要处理的数据")
        flow_list = current_pg.execute_query("""select flow.data as data, flow.name as flow_name, folder.name as folder from flow 
                left join folder on flow.user_id=folder.user_id and flow.folder_id=folder.id WHERE flow.is_component=False 
                and jsonb_path_exists(data::jsonb,
              '$.nodes[*] ? (
                @.id starts with "RunFlow"
              )'
            ) and flow.user_id=%s and RIGHT(folder.name, 1)='*';""", (current_user_id,), with_columns=True)
        current_pg.commit()
        my_table = st.dataframe([])
        length = len(flow_list)

        wait_update = {}
        runflow_flow_map = defaultdict(set)
        for _index, run_flow in enumerate(flow_list):
            # 一个含有run_flow的flow
            nodes = run_flow['data']['nodes']
            name = run_flow['flow_name']
            folder_name = run_flow['folder']
            my_table.add_rows(pd.DataFrame([{"目录": folder_name, "流程": name, "runflow要调用的flow": ""}]))

            run_flow_new_template_keys = dict()

            replace_edge_target = {}
            my_bar.progress(round(_index / length, 1), text=f"正在处理  :red[{name}]")
            # 更新节点
            for node in nodes:
                if node['id'].startswith("RunFlow"):
                    # 这是该flow的一个run_flow
                    template = node["data"]["node"]["template"]
                    # original_template_key = set(template.keys())
                    flow_name_selected = template["flow_name_selected"]["value"]
                    runflow_flow_map[name].add(flow_name_selected)

                    # 找到run_flow里面被引用的flow
                    vertex_data = current_pg.execute_query("select data from flow where name=%s and user_id=%s",
                                                           (flow_name_selected, current_user_id))
                    current_pg.commit()
                    if not vertex_data:
                        my_table.add_rows([{"目录": "", "流程": "", "runflow要调用的flow": f"🚫{flow_name_selected}"}])
                        st.error(f"目录:👉{folder_name}👈的flow:👉{name}👈找不到被引用的flow:👉{flow_name_selected}👈")
                        continue
                    my_table.add_rows([{"目录": "", "流程": "", "runflow要调用的flow": f"{flow_name_selected}"}])
                    vertex_nodes = vertex_data[0][0]['nodes']
                    new_fields = []
                    # 遍历被引用的flow
                    for vertex_node in vertex_nodes:
                        # 寻找输入节点
                        is_input = any(input_name in vertex_node['id'] for input_name in self.input_id)
                        if not is_input:
                            continue
                        # 找到输入节点
                        field_template = vertex_node['data']['node']['template']
                        field_order = vertex_node['data'].get("node", {}).get("field_order", [])
                        if field_order and field_template:
                            new_vertex_inputs = [
                                dict(
                                    {
                                        **field_template[input_name],
                                        "display_name": vertex_node['data'].get("node", {})[
                                                            'display_name'] + " - " +
                                                        field_template[input_name][
                                                            "display_name"],
                                        "name": f"{vertex_node['id']}~{input_name}",
                                        "tool_mode": not (field_template[input_name].get("advanced", False)),
                                    }
                                )
                                for input_name in field_order
                                if input_name in field_template
                            ]
                            new_fields += new_vertex_inputs
                    old_fields = [
                        field
                        for field in template
                        if field not in [new_field["name"] for new_field in new_fields] + ["code", "_type",
                                                                                           "flow_name_selected",
                                                                                           "session_id"]
                    ]

                    # 如果旧节点的和新节点的数量都为1且display_name相同，可以把线直接连起来
                    if len(new_fields) == len(old_fields) == 1:
                        if new_fields[0]["display_name"] == old_fields[0]["display_name"]:
                            replace_edge_target[old_fields[0]['name']] = new_fields[0]['name']

                    # 删除字段
                    for field in old_fields:
                        template.pop(field, None)
                    # 新增字段
                    for field in new_fields:
                        template[field["name"]] = field
                    # new_template_key = set(template.keys())
                    # if u_in := original_template_key.symmetric_difference(new_template_key):
                    #     need_update = True
                    run_flow_new_template_keys[node["id"]] = set(template.keys())

            # 更新线
            new_edges = []
            for edge in run_flow['data']['edges']:
                # 如果该线的target是run_flow
                if new_keys := run_flow_new_template_keys.get(edge['target']):
                    old_target_key = edge["data"]["targetHandle"]["fieldName"]
                    # 如果该线的target已经不存在了
                    if old_target_key not in new_keys:
                        # 如果该线的target可以被替换
                        if new_target_key := replace_edge_target.get(old_target_key):
                            edge["data"]["targetHandle"]["fieldName"] = new_target_key
                        else:
                            # 删除该线
                            continue
                new_edges.append(edge)
            run_flow['data']['edges'] = new_edges

            wait_update[name] = json.dumps(run_flow['data'])
        my_bar.empty()
        return wait_update, runflow_flow_map

    @st.fragment
    def execute_update_run_flow(self, wait_update, current_pg, current_user_id, with_button=True):
        if with_button and not st.button("提交更新", type="primary", key="submit7"):
            return
        for name, data in wait_update.items():
            current_pg.execute_update("update flow set data=%s where name=%s and user_id=%s",
                                      (data, name, current_user_id))
            current_pg.commit()
            if with_button:
                st.success(f"{name} 更新成功", icon='🎉')
        st.toast("刷新完成", icon='🎉')
        st.balloons()

    @st.fragment
    def flush_label(self):
        st.caption("本页面主要是用于刷新Langfuse2的label标签，将不是指定标签的数据刷新到指定标签上去")
        selection = st.segmented_control(
            "1.请选择环境", (("开发", "dev"), ("测试", "test"), ("beta", "beta"), ("正式", "pro")),
            selection_mode="single", format_func=lambda x: x[0], key="flush_label"
        )
        if selection is None:
            return
        new_label = st.pills("2.请选择要更新标签", ['dev', 'test', 'stage', 'production'], selection_mode="single",
                             key="future_label")
        if not new_label:
            return
        current_env = selection[1]
        current_user_info = get_env_pg_user_info(current_env)
        st.warning(
            f"您当前选择的环境是:**:red[{current_env}]**，"
            f"您当前选择的用户是:**:red[{current_user_info.username}]**，"
            f"您要更新的标签是:**:red[{new_label}]**，"f""
        )
        current_pg = get_env_pg(current_env)
        current_user_id = get_user_id(current_env, current_user_info.username)

        with st.spinner("数据加载中...", show_time=True):
            flow_list = current_pg.execute_query(f"""select flow.id, flow.name, flow.data, folder.name as folder from flow 
             left join folder on flow.user_id=folder.user_id and flow.folder_id=folder.id WHERE flow.is_component=False 
             and jsonb_path_exists(data::jsonb,
                  '$.nodes[*] ? (
                    @.id starts with "LangfusePrompt2" && @.data.node.template.label.value != "{new_label}"
                  )'
                ) and flow.user_id=%s and RIGHT(folder.name, 1)='*'""", (current_user_id,), with_columns=True)
            current_pg.commit()
        table = st.dataframe([], use_container_width=True, hide_index=False)

        wait_update_flow = {}
        show_warning = False
        for flow in flow_list:
            flow_id = flow['id']
            flow_name = flow['name']
            flow_data = flow['data']
            folder_name = flow['folder']
            table.add_rows([{
                '目录名': folder_name,
                'flow_name': "",
                '提示词名': "",
                '原始标签': "",
                '待更新标签': "",
            }])

            for node in flow_data['nodes']:
                if node['id'].startswith("LangfusePrompt2"):
                    node_template = node['data']['node']['template']
                    if 'label' not in node_template:
                        # todo 发现异常数据
                        st.error(f"发现异常数据，flow_id={flow_id},flow_name={flow_name},node_id={node['id']}")
                        continue

                    original_label = node_template['label']['value']
                    original_prompt_name = node_template['self_prompt_name']['value'] or node_template['prompt_name'][
                        'value']
                    if original_prompt_name:
                        show_warning = True
                    table.add_rows([{
                        '目录名': "",
                        'flow_name': flow_name,
                        '提示词名': original_prompt_name,
                        '原始标签': original_label,
                        '待更新标签': f'{new_label}',
                    }])
                    node_template['label']['value'] = new_label
            wait_update_flow[flow_id] = flow_data
        if show_warning:
            st.warning("部分提示词名没有值的是由于它的输入是前一个组件传的", icon="⚠️")
        self.update_flow_label(wait_update_flow, current_pg, new_label)

    @st.fragment
    def update_flow_label(self, wait_update_flow, current_pg, new_label):
        if not wait_update_flow:
            st.warning(f"没有找到标签不是{new_label}的数据", icon='⚠️')
            return
        if st.button("更新", type="primary", key="update_flow_label"):
            my_bar = st.progress(0.0, text="开始刷新数据")
            length = len(wait_update_flow)
            begin = 0
            for flow_id, flow_data in wait_update_flow.items():
                my_bar.progress(round(begin / length, 1), text=f"开始刷新数据{flow_id}")
                current_pg.execute_update("update flow set data=%s where id=%s",
                                          (json.dumps(flow_data), flow_id))
                current_pg.commit()
                st.success(f"{flow_id}更新成功", icon='🎉')
                begin += 1
            my_bar.empty()
            st.toast(f"更新成功", icon='🎉')

    @staticmethod
    def check_description(description):
        return ['background-color: red' if not re.match(r'\[.*?].*?-\d{8}-\d{2}:\d{2}', des) else '' for des in
                description]

    @st.fragment
    def show_data(self, df, future_env):
        show_data = df.iloc[:, :-1]
        styled_df = show_data.style.apply(self.check_description, subset=['description'])
        tt = st.dataframe(data=styled_df, selection_mode='multi-row', use_container_width=True, hide_index=False,
                          column_order=['folder_name', 'name', 'description', 'updated_at'], on_select='rerun')

        if tt['selection']['rows'] and st.button("确认", type="primary", key="submit1"):
            selected = df.iloc[tt['selection']['rows']]
            self.online_flow(selected, future_env)
            return selected

    # 上线流程
    @st.dialog("上线流程", width="large")
    def online_flow(self, selected, future_env):
        st.caption("你将把下面的流程部署到生产环境，点击提交按钮以执行")
        st.write(selected.iloc[:, :3])
        future_user_info = get_env_pg_user_info(future_env)
        # 获取token
        langflow_token = login_langflow(future_user_info.username, future_user_info.password, future_user_info.url)

        if st.button("提交上线", type="primary", key="submit2"):
            self.submit(selected, langflow_token, future_env, future_user_info)

    @st.fragment
    def submit(self, selected, langflow_token, future_env, future_user_info):
        future_user_id = get_user_id(future_env, future_user_info.username)
        last_version, new_version = self.get_version(future_env, future_user_id)
        st.header(
            "请确认本次操作版本号: :red[{}]  上个操作版本号为: :blue[{}]".format(new_version,
                                                                                 last_version or "20150618.0"))
        # 获取目录信息
        selected_dirs = selected['folder_name'].unique().tolist()
        # 查看目录是否存在
        dir_info = get_folder_info_by_sql(future_env, user_id=future_user_id, with_backups=True)
        dir_dict = dict(dir_info)  # {name:id}
        for selected_dir in selected_dirs:
            if selected_dir not in dir_dict:
                st.error(f"目录{selected_dir}不存在，请先创建目录")
                if st.button(f"创建目录->{selected_dir}", type="primary", key="submit6"):
                    dir_id = create_folder_by_api(langflow_token, selected_dir, future_user_info.url)
                    if not dir_id:
                        return
                    dir_dict[selected_dir] = dir_id
                    st.rerun()
                else:
                    return

        if not st.button(
                f"继续将flow提交到 {future_user_info.username} 用户下的 {selected_dirs} 目录",
                type="primary", key="submit3"):
            return
        self._backup_langfuse(last_version, new_version, future_env)
        back_dir_id = dir_dict.get("备份", None)
        if not back_dir_id:
            # 创建备份目录
            back_dir_id = create_folder_by_api(langflow_token, "备份", future_user_info.url)
            if not back_dir_id:
                return
        data = selected.to_dict(orient="records")
        percent_complete = 0
        my_bar = st.progress(percent_complete, text="正在开始导入...")
        future_pg = get_env_pg(future_env)

        success_info = []
        length = len(data)
        st.write(f"一共有{length}条数据")
        back_columns = ['data', 'name', 'description', 'user_id', 'is_component', 'updated_at', 'icon',
                        'icon_bg_color',
                        'folder_id', 'endpoint_name', 'webhook', 'gradient', 'tags', 'locked', 'fs_path',
                        'access_type',
                        'mcp_enabled', 'action_name', 'action_description', 'version', 'environment', 'created_at',
                        'is_exist', 'old_id']

        for one_index, one in enumerate(data):
            folder_id = dir_dict[one['folder_name']]
            percent_complete = round(one_index / length, 1)
            exist = future_pg.execute_query(f"select * from flow where name=%s and user_id=%s",
                                            (one['name'], future_user_id), with_columns=True)
            future_pg.commit()
            if len(exist) > 0:
                exist_flow = exist[0]
                exist_id = exist_flow['id']
                new_id = str(uuid4())
                # 备份存在的flow
                back_name = f"{one['name']}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                my_bar.progress(percent_complete, text=f"开始备份 {back_name}...")
                future_pg.execute_update(f"update flow set id=%s, name=%s,folder_id=%s where id=%s", (
                    new_id, back_name, back_dir_id, exist_id))
                future_pg.commit()

                # 保存版本数据
                values = [exist_flow.get(c) for c in back_columns]
                values = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in values]
                values[-5] = new_version
                values[-4] = future_env
                values[-3] = datetime.now()
                values[-2] = True
                values[-1] = exist_id
                self.sqlite_op.execute_update(
                    f"insert into flow_history({','.join(back_columns)}) values({','.join(['?'] * len(back_columns))})",
                    tuple(values))
                self.sqlite_op.commit()
                my_bar.progress(percent_complete, text=f"备份 {back_name} 成功，正在导入 {one['name']} ...")

                # 新建flow
                for k in exist_flow.keys():
                    if up_value := one.get(k):
                        exist_flow[k] = up_value
                exist_flow['updated_at'] = datetime.now()
                # 修正一些数据
                flow_data = one['data']
                for node in flow_data['nodes']:
                    node_template = node["data"]["node"]["template"]
                    # 修改langfuse2的label
                    if node['id'].startswith("LangfusePrompt2"):
                        if 'label' not in node_template:
                            # todo 发现异常数据
                            st.warning(f"{one['name']}的LangfusePrompt2有异常数据，请检查label")
                            continue
                        new_label = self.env_label[future_env]
                        node_template['label']['value'] = new_label
                    elif node['id'].startswith("RunFlow"):
                        value = node_template["flow_name_selected"]["value"]
                        node_template["flow_name_selected"]["options"] = [value]
                    elif node['id'].startswith("ChatInput"):
                        if node["data"]['node']['display_name'] == 'FileSelect':
                            if 'files' not in node_template:
                                st.error(f"{one['name']}的ChatInput有异常数据，请检查files")
                                continue
                            node_template['files']['file_path'] = []
                            node_template['files']['value'] = ""

                exist_flow['data'] = json.dumps(flow_data)
                exist_flow['folder_id'] = folder_id
                exist_flow['tags'] = json.dumps(exist_flow['tags']) if exist_flow['tags'] is not None else None
                columns, values = zip(*exist_flow.items())
                values = list(values)
                new_columns = ", ".join(columns)
                sql = f"insert into flow({new_columns}) values({', '.join(['%s'] * len(values))})"
                try:
                    future_pg.execute_update(sql, tuple(values))
                except Exception as e:
                    if "unique_flow_endpoint_name" in str(e):
                        index_ = columns.index("endpoint_name")
                        values[index_] = None
                        future_pg.execute_update(sql, tuple(values))
                success_info.append((exist_flow['name'], exist_id))
            else:
                my_bar.progress(percent_complete, text=f"正在导入 {one['name']} ...")
                response = create_new_flow_by_api(langflow_token, folder_id, one, future_user_info.url)
                if response:
                    success_info.append((response['name'], response['id']))

                    # 保存版本数据
                    values = [response.get(c) for c in back_columns]
                    values = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in values]
                    values[-5] = new_version
                    values[-4] = future_env
                    values[-3] = datetime.now()
                    values[-2] = False
                    values[-1] = response['id']
                    self.sqlite_op.execute_update(
                        f"insert into flow_history({','.join(back_columns)}) values({','.join(['?'] * len(back_columns))})",
                        tuple(values))
                    self.sqlite_op.commit()

        my_bar.empty()
        st.success("导入完成 \n" +
                   "|  flow_name   | flow_id  |\n" +
                   "| :------: | :------: |\n" +
                   "\n".join([f"|**:red[{name}]**  | {_id}|" for name, _id in success_info]))

    def _backup_langfuse(self, old_version, new_version, env):
        # 先备份langfuse数据
        fuse_pg = get_fuse_pg()
        new_fuse_data = fuse_pg.execute_query(f"select version,label,p.name as name from {self.fuse_table} p,"
                                              f"unnest(p.labels) AS label where project_id='{self.fuse_project_id}' "
                                              f"AND label='{env}' order by name", with_columns=True)
        fuse_pg.commit()
        sqlite = self.sqlite_op
        # 如果新版本数据已经存在了，为了不干扰，直接删除
        if sqlite.execute_query(f"select count(*) from fuse_history where label='{env}' and history>='{new_version}'"):
            sqlite.execute_update(f"delete from fuse_history where label='{env}' and history>='{new_version}'",
                                  autocommit=False)

        max_old_history = sqlite.execute_query(f"select max(history) as max_old_history from fuse_history "
                                               f"where label='{env}' and history<='{old_version}' and operation!='same'", )
        sqlite.commit()
        st.write("开始备份langfuse数据")

        if not max_old_history[0].get('max_old_history'):
            try:
                for one in new_fuse_data:
                    sqlite.execute_update("insert into fuse_history(id, history, name, version, label, operation) values (?,?,?,?,?,?)",
                                          (str(uuid.uuid4()), '20250618.0', one['name'], one['version'], one['label'], 'init'),
                                          autocommit=False)
                sqlite.execute_update("insert into fuse_history(id, history, label, operation) values (?,?,?,?)",
                                      (str(uuid.uuid4()), new_version, env, 'same'), autocommit=False)
                sqlite.commit()
            except Exception as e:
                sqlite.rollback()
                st.error("备份fuse_history失败，请重试: {}".format(e))
                raise e
        else:
            try:
                max_old_history = max_old_history[0]['max_old_history']
                old_fuse_data = sqlite.execute_query(
                    (f"select name, version, max(history) as history, operation from fuse_history where name is not null "
                     f"and label='{env}' and history<='{max_old_history}' group by name"))
                sqlite.commit()

                # 测试数据
                # new_fuse_data.pop()
                # new_fuse_data[33]['version'] = 31
                # new_fuse_data.append({'name': 'root1', 'label': env, 'version': 9})
                # st.write(new_fuse_data[33])

                new_fuse_dict = {one['name']: one['version'] for one in new_fuse_data}
                old_fuse_dict = {one['name']: one['version'] for one in old_fuse_data if one['operation'] not in ('same', 'remove')}
                diff = DeepDiff(old_fuse_dict, new_fuse_dict, view="text")
                if diff:
                    st.write('**fuse数据变化如下：**')
                    st.write(diff)
                    name_pattern = re.compile(r'root\[\'(.*?)\']')
                    for change_type, changes in diff.items():
                        match change_type:
                            case "dictionary_item_removed":
                                removed = name_pattern.findall(str(changes))
                                # 旧的有新的没有
                                for remove in removed:
                                    old_v = old_fuse_dict[remove]
                                    sqlite.execute_update(
                                        "insert into fuse_history(name, label, version, operation, history) values (?,?,?,?,?)",
                                        (remove, env, old_v, 'remove', new_version), autocommit=False)
                            case "dictionary_item_added":
                                added = name_pattern.findall(str(changes))
                                # 新的有旧的没有
                                for add in added:
                                    new_v = new_fuse_dict[add]
                                    sqlite.execute_update(
                                        "insert into fuse_history(name, label, version, operation, history) values (?,?,?,?,?)",
                                        (add, env, new_v, 'add', new_version), autocommit=False)
                            case "values_changed":
                                if 'root' in changes:
                                    for name, old_v in changes['root']['old_value'].items():
                                        sqlite.execute_update(
                                            "insert into fuse_history(name, label, version, operation, history) values (?,?,?,?,?)",
                                            (name, env, old_v, 'change', new_version), autocommit=False)
                                else:
                                    for key, value in changes.items():
                                        name = name_pattern.findall(str(key))[0]
                                        old_v = value['old_value']
                                        sqlite.execute_update(
                                            "insert into fuse_history(name, label, version, operation, history) values (?,?,?,?,?)",
                                            (name, env, old_v, 'change', new_version), autocommit=False)
                else:
                    st.warning("langfuse数据未发生变化")
                    sqlite.execute_update("insert into fuse_history(label, operation, history) values (?,?,?)",
                                          (env, 'same', new_version), autocommit=False)
                sqlite.commit()

            except Exception as e:
                sqlite.rollback()
                st.error("备份fuse_history失败：{}".format(e))
                raise e

        st.success("备份langfuse数据成功")


DeploymentPage().deployment_page()
