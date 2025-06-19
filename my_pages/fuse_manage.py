import random
from collections import defaultdict

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from threading import RLock

import streamlit as st

from util import get_pg, get_fuse_pg, get_sqlite

_lock = RLock()

url = "http://fuse.letuzhixing.com"
all_in_one_public = "pk-lf-045477fd-bf29-4bef-a83b-e534ed592bd3"
all_in_one_secret = "sk-lf-fb3d1c07-68e7-445d-88b7-5578a12228c0"

chinese_public_key = "pk-lf-74571659-dc72-4725-96b0-ddfe622de97b"
chinese_secret_key = "sk-lf-89cb4ed3-078b-4552-82f2-00335420dfb6"
mathematics_public_key = "pk-lf-45ce220c-54f7-4dba-ac75-458682128dd2"
mathematics_secret_key = "sk-lf-4535b803-ab6f-498c-8611-1608ce8d3aa9"
english_public_key = "pk-lf-68636780-0710-4ebf-8400-a37ef69c9c9d"
english_secret_key = "sk-lf-d6c06cec-a3c2-49d8-b7bd-a5edfd30c08e"
physics_public_key = "pk-lf-46314f28-473a-4d4a-9cc8-25d75e0fc14f"
physics_secret_key = "sk-lf-b05ff262-759e-4661-8dfd-60d144fcf85b"
chemistry_public_key = "pk-lf-31705092-ed65-438c-a515-2db4c3fb18d3"
chemistry_secret_key = "sk-lf-632d1631-adff-4538-be1b-33e25a804936"
biology_public_key = "pk-lf-1626a96d-1a9d-40cd-b5af-4e6ac407dc44"
biology_secret_key = "sk-lf-594a8591-e380-47fd-9fc1-ef1ecb0a18bf"
history_public_key = "pk-lf-cc58080e-0d7c-4f66-85cf-069795355d24"
history_secret_key = "sk-lf-36f1207b-52d6-4cf9-8fe7-800e97890654"
geography_public_key = "pk-lf-f52aa8e0-edd8-40cf-9ce3-7052a59db9f7"
geography_secret_key = "sk-lf-7a160e07-901f-46c2-a0c6-96396cea103d"
politic_public_key = "pk-lf-d913103d-5683-4bed-b90e-569414964444"
politic_secret_key = "sk-lf-d89df71e-4704-45e4-bff3-02bd5b6ace91"

map_dict = {
    "语文": (chinese_public_key, chinese_secret_key),
    "数学": (mathematics_public_key, mathematics_secret_key),
    "英语": (english_public_key, english_secret_key),
    "物理": (physics_public_key, physics_secret_key),
    "化学": (chemistry_public_key, chemistry_secret_key),
    "生物": (biology_public_key, biology_secret_key),
    "历史": (history_public_key, history_secret_key),
    "地理": (geography_public_key, geography_secret_key),
    "政治": (politic_public_key, politic_secret_key),
}


def get_fuse_prompt_list(public_key, secret_key, name=None, label=None, tag=None):
    page = 1
    limit = 50
    while True:
        response = requests.get(
            f"{url}/api/public/v2/prompts",
            auth=HTTPBasicAuth(public_key, secret_key),
            params={
                "name": name,
                "label": label,
                "tag": tag,
                "page": page,
                "limit": limit,
            },
        )
        res = response.json()
        data = res["data"]
        for item in data:
            yield item
        total_pages = res["pagination"]['totalPages']
        if page >= total_pages:
            break
        page += 1


def create_fuse_prompt(data):
    response = requests.post(
        f"{url}/api/public/v2/prompts",
        auth=HTTPBasicAuth(all_in_one_public, all_in_one_secret),
        json=data
    )
    res = response.json()
    assert "id" in res
    return res


def get_fuse_prompt(public_key, secret_key, name, version=None, label=None):
    response = requests.get(
        f"{url}/api/public/v2/prompts/{name}",
        auth=HTTPBasicAuth(public_key, secret_key),
        params={
            "label": label,
            "version": version,
        },
    )
    res = response.json()
    return res


def get_project_info(public_key, secret_key):
    response = requests.get(
        f"{url}/api/public/projects",
        auth=HTTPBasicAuth(public_key, secret_key),
    )
    res = response.json()
    return res['data'][0]


def continue_create_fuse_prompt(data, label, project_name):
    old_data = get_fuse_prompt(all_in_one_public, all_in_one_secret, data['name'], label=label)
    if old_data.get('error', "") == 'LangfuseNotFoundError':
        return True
    old_commit_message = old_data['commitMessage']
    new_commit_message = data['commitMessage']
    if old_commit_message != new_commit_message:
        print(f"project_name: {project_name}, 更新 {data['name']}")
        return True
    if data['prompt'] != old_data['prompt']:
        print(f"project_name: {project_name}, 更新 {data['name']}")
        return True
    return False


def main():
    label = "dev"
    for subject, (public_key, secret_key) in map_dict.items():
        # 获取项目信息
        project_info = get_project_info(public_key, secret_key)
        project_name = project_info['name']
        # 获取fuse_prompt
        for item in get_fuse_prompt_list(public_key, secret_key, label=label):
            name = item['name']
            versions = item['versions']
            for version in versions:
                fuse_prompt = get_fuse_prompt(public_key, secret_key, name, version)
                new_data = {
                    "type": "text",
                    "name": fuse_prompt["name"],
                    "prompt": fuse_prompt["prompt"],
                    "labels": fuse_prompt["labels"],
                    "tags": [subject],
                    "commitMessage": subject
                }
                if subject not in new_data['name']:
                    new_data['name'] = f"{subject}_{new_data['name']}"
                if continue_create_fuse_prompt(new_data, label=label, project_name=project_name):
                    create_fuse_prompt(new_data)
                    # pass


def update_prompt_labels(name, version, dest_labels):
    update_url = f"{url}/api/public/v2/prompts/{name}/versions/{version}"
    response = requests.patch(
        update_url, auth=HTTPBasicAuth(all_in_one_public, all_in_one_secret),
        json={
            "newLabels": [
                dest_labels
            ]
        }
    )
    if response.status_code != 200:
        st.error(response.text)
        return False
    return "id" in response.json()


def update_latest_to_dev(get_labels="dev", dest_labels="test"):
    names = []
    for item in get_fuse_prompt_list(all_in_one_public, all_in_one_secret, label=get_labels):
        if dest_labels not in item['labels']:
            version = item['versions'][0]
            names.append(item['name'])
            update_prompt_labels(item['name'], version, dest_labels)
    for name in names:
        print(name)


class LangfuseManage:
    def __init__(self, ):
        self.public_key = all_in_one_public
        self.secret_key = all_in_one_secret
        self.url = "http://fuse.letuzhixing.com"
        self.tables = "prompts"
        self.project_id = "cmbahi0nx00ympp085f23z64w"

    def manage_page(self):
        st.header("Langfuse管理")
        self.statistics_data()
        self.update_prompt_labels()

    def statistics_data(self):
        if st.button("刷新数据"):
            st.rerun()
        st.subheader("提示词版本统计")
        pg = get_fuse_pg()
        results = pg.execute_query(
            f"select name, count(*) as v from {self.tables} where project_id='{self.project_id}' group by name")
        pg.commit()
        group_item = {}
        for row in results:
            name = row[0]
            version = row[1]
            group_name = name.split("_")[0]
            if group_name not in group_item:
                group_item[group_name] = [{"name": name, "version": version, "color": self.random_colormap_color()}]
            else:
                group_item[group_name].append(
                    {"name": name, "version": version, "color": group_item[group_name][0]['color']})

        chart_data = pd.DataFrame([item for key in group_item for item in group_item[key]],
                                  columns=["name", "version", 'color'])
        st.bar_chart(chart_data, x="name", y="version", color='color', stack='layered')
        self.show_fuse_version_data()

    def show_fuse_version_data(self):
        pg = get_fuse_pg()
        results = pg.execute_query(
            f"select distinct unnest(labels) as label from {self.tables} where project_id='{self.project_id}'")
        columns = [row[0] for row in results]

        all_data = pg.execute_query(f"select version,label,p.name as name from {self.tables} p,"
                                    f"unnest(p.labels) AS label where project_id='{self.project_id}' "
                                    f"AND label in {tuple(columns)} order by name", with_columns=True)

        max_version = pg.execute_query(f"select max(version) from {self.tables} where project_id='{self.project_id}'")
        pg.commit()
        name_dict = defaultdict(dict)
        for row in all_data:
            name = row['name']
            if name not in name_dict:
                name_dict[name]['name'] = name
                name_dict[name]['prefix'] = name.split('_')[0]
                name_dict[name]['version'] = [0] * len(columns)
                for col in columns:
                    name_dict[name][col] = 0
            name_dict[name][row['label']] = row['version']
            index_ = columns.index(row['label'])
            name_dict[name]['version'][index_] = row.get('version', 0)
        st.data_editor(pd.DataFrame(name_dict.values()), use_container_width=True, column_config={
            "version": st.column_config.BarChartColumn(
                ", ".join(columns),
                help="版本",
                y_min=0,
                y_max=max_version[0][0],
                width='small',
            ), **{
                col: st.column_config.ProgressColumn(help=col, width='small', min_value=0, max_value=max_version[0][0],
                                                     format="plain") for col in
                columns}
        })


    @staticmethod
    def random_colormap_color():
        return "#{:06x}".format(random.randint(0, 0xFFFFFF)).upper()

    @st.fragment
    def update_prompt_labels(self):
        st.subheader("提示词标签版本同步")
        st.caption("把fuse的提示词标签从源标签所对应的版本更新到目标标签所对应的版本，即源标签版本和目标标签版本对齐")
        support_labels = ["dev", "test", "stage", "production"]
        cols = st.columns(6)
        with cols[0]:
            src_labels = st.selectbox("源标签", support_labels)
        with cols[1]:
            dest_labels = st.selectbox("目标标签", {x for x in support_labels if x != src_labels})
        if src_labels == dest_labels:
            st.error("源标签和目标标签不能相同")
            return
        columns = st.columns(3)
        with columns[0]:
            bt = st.button(f"把源标签:blue[{src_labels}]更新到目标标签:blue[{dest_labels}]同一版本", type='primary',
                           icon="🔥")
        with columns[1]:
            query_data = st.checkbox("仅查阅数据", value=True)
        if not bt:
            return
        st.warning("已经过滤了{}标签和{}标签在同一版本的数据".format(src_labels, dest_labels))
        tables = st.table()
        with st.spinner("正在更新数据，请稍等...", show_time=True):
            for item in get_fuse_prompt_list(all_in_one_public, all_in_one_secret, label=src_labels):
                if dest_labels not in item['labels']:
                    version = item['versions'][0]
                    tables.add_rows(
                        [{"name": item['name'], "版本": version, '原有标签': ','.join(item['labels']),
                          '新增标签': dest_labels}]
                    )
                    if not query_data:
                        if update_prompt_labels(item['name'], version, dest_labels):
                            st.toast(f"{item['name']} 更新成功", icon='🎉')
                        else:
                            return
            if not query_data:
                st.success("更新完成", icon='🎉')
            else:
                st.success("数据已经过滤完成", icon='🎉')


LangfuseManage().manage_page()

# if __name__ == '__main__':
#     update_latest_to_dev()
