import re
from collections import defaultdict

import streamlit as st
from deepdiff import DeepDiff

from util import init_pg, init_mysql, get_env_mysql, get_env_pg, get_env_pg_user_info, get_user_id


class HHDataPage:
    def __init__(self):
        self.table = "langflow_config"

    def manage_page(self):
        init_pg()
        init_mysql()
        st.header("后台数据管理")
        self.show_info()

    @staticmethod
    def __select_env():
        return st.segmented_control(
            "请选择环境", (("开发", "dev"), ("测试", "test"), ("beta", "beta"), ("正式", "pro")),
            selection_mode="single", format_func=lambda x: x[0], key="flush_rn_flow"
        )

    @st.fragment
    def show_info(self):
        st.subheader("1.数据展示")
        select_env = self.__select_env()
        if not select_env:
            return
        env = select_env[1]
        mysql = get_env_mysql(env)
        src_data = mysql.execute_query(f"select `id`,`flow_id`,`name`,`desc` from {self.table} order by name")
        mysql.commit()
        st.dataframe(src_data)
        self.compare_diff_env(select_env, src_data)

    def compare_diff_env(self, src_env, src_data):
        if not src_env:
            return
        st.subheader("2.数据对比")
        column1, _ = st.columns(2)
        with column1:
            option = (("开发", "dev"), ("测试", "test"), ("beta", "beta"), ("正式", "pro"))
            dst_env = st.selectbox("请选择目标环境",
                                   [x for x in option if x[1] != src_env[1]],
                                   format_func=lambda x: x[0])

        if not dst_env:
            return
        st.write(f"源环境是:**:red[{src_env[0]}]**，目标环境是:**:red[{dst_env[0]}]**")
        st.caption(f"备注的意思的 **:red[{dst_env[0]}]** 环境相比于 **:red[{src_env[0]}]** 环境的差异")
        dst_mysql = get_env_mysql(dst_env[1])
        sql = f"select `id`,`flow_id`,`name`,`desc` from {self.table}"
        dst_data = dst_mysql.execute_query(sql)
        dst_mysql.commit()
        diff = DeepDiff(src_data, dst_data, group_by='name', ignore_order=True, exclude_paths="root['id']", view="text")
        self.display_diff(diff, src_data, dst_data, src_env[0], dst_env[0])

    def display_diff(self, diff_result, obj1, obj2, src_env, dst_env):
        """展示单层JSON数据的差异（表格形式，红色标记不同项）"""
        if not diff_result:
            st.write("无差异")
            return
        removed = []
        added = []
        changed = defaultdict(list)
        name_pattern = re.compile(r'root\[\'(.*?)\']')
        key_pattern = re.compile(r'root\[\'.*?\']\[\'(.*?)\']')

        for change_type, changes in diff_result.items():
            match change_type:
                case "dictionary_item_removed":
                    removed = name_pattern.findall(str(changes))
                case "dictionary_item_added":
                    added = name_pattern.findall(str(changes))
                case "values_changed":
                    for key, value in changes.items():
                        name = name_pattern.findall(str(key))[0]
                        column = key_pattern.findall(str(key))[0]
                        changed[name].append(column)

        show_table = st.table([])
        obj1_dic = {obj['name']: obj for obj in obj1}
        update_data = {
            "need_add": [],
            "need_delete": [],
            "need_update": [],
        }
        for remove in removed:
            show_table.add_rows([{
                "compare": f"{src_env}\n\n{dst_env}",
                "id": f"{obj1_dic[remove].get('id', "\-")}\n\n\-",
                "name": f":violet-background[{obj1_dic[remove].get('name', "\-")}]",
                "desc": f"{obj1_dic[remove].get('desc', "\-")}\n\n\-",
                "备注": f":violet-badge[少]"
            }])
            update_data["need_add"].append(obj1_dic[remove])
        for one in obj2:
            name = one["name"]
            another = obj1_dic.get(name, {})
            new = {
                "compare": f"{src_env}\n\n{dst_env}",
                "id": f"{another.get('id', "\-")}\n\n{one['id']}",
                "name": f"{one['name']}",
                "desc": f"{another.get('desc', "\-")}\n\n{one['desc']}",
                "备注": ""
            }
            if name in removed:
                new["备注"] = f":violet-badge[少]"
                new['name'] = f":violet-background[{one['name']}]"
                update_data["need_add"].append(another)
            if name in added:
                new["备注"] = f":red-badge[多]"
                new['name'] = f":red-background[{one['name']}]"
                update_data["need_delete"].append(one)
            elif name in changed:
                new["备注"] = f":blue-badge[变]"
                new['name'] = f":blue-background[{one['name']}]"
                changed_column = changed[name]
                for column in changed_column:
                    new[column] = f"{another.get(column, "")}\n\n:blue[{one[column]}]"
                update_data["need_update"].append({"id": one["id"], "name": one["name"], "desc": another["desc"], })
            show_table.add_rows([new])
        if changed or added or removed:
            self.data_synch(src_env, dst_env, update_data)

    def data_synch(self, src_env, dst_env, update_data):
        st.subheader("3.数据同步")
        st.caption(f"将源环境 **:red[{src_env}]** 的数据同步到目标环境 **:red[{dst_env}]** ")
        dst_mysql = get_env_mysql(dst_env)

        if st.button("同步", use_container_width=False, type="primary"):
            for key, value in update_data.items():
                if not value:
                    continue
                match key:
                    case "need_add":
                        st.write("需要新增的数据")
                        for one in value:
                            flow_id = self.get_flow_id(dst_env, one['desc'])
                            if not flow_id:
                                st.error(f"{one['desc']}获取flow_id失败")
                                dst_mysql.rollback()
                                return
                            sql = f"insert into {self.table} set flow_id=%s, name=%s, desc=%s, updated_at=now()"
                            dst_mysql.execute_update(sql, (flow_id[0], one['name'], one['desc']))
                            st.badge(one['name'], icon=":material/check:", color="green")
                    case "need_delete":
                        st.write("需要删除的数据")
                        for one in value:
                            sql = f"delete from {self.table} where id=%s"
                            dst_mysql.execute_update(sql, (one['id'], ))
                            st.badge(one['name'], icon=":material/check:", color="green")
                    case "need_update":
                        st.write("需要更新的数据")
                        for one in value:
                            flow_id = self.get_flow_id(dst_env, one['desc'])
                            if not flow_id:
                                st.error(f"{one['desc']}获取flow_id失败")
                                dst_mysql.rollback()
                                return
                            sql = f"update {self.table} set flow_id=%s, desc=%s where id=%s"
                            dst_mysql.execute_update(sql, (flow_id[0], one['desc'], one['id']))
                            st.badge(one['name'], icon=":material/check:", color="green")

            dst_mysql.commit()
            st.success("同步成功")
            st.balloons()

    def get_flow_id(self, env, flow_name):
        pg = get_env_pg(env)
        userinfo = get_env_pg_user_info(env)
        current_user_id = get_user_id(env, userinfo.username)
        flow_id = pg.execute_query(f"select id from flow where name='{flow_name}' and user_id='{current_user_id}'")
        pg.commit()
        return flow_id


HHDataPage().manage_page()
