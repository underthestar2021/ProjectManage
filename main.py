import streamlit as st

USER_DB = {
    "admin": {"password": "admin123", "role": "admin"},
}


def login_page():
    st.title("系统登录")

    with st.form("login_form"):
        username = st.text_input("用户名")
        password = st.text_input("密码", type="password")
        submitted = st.form_submit_button("登录")

        if submitted:
            if username in USER_DB and USER_DB[username]["password"] == password:
                st.session_state.logged_in = True
                st.session_state.username = username
                st.session_state.role = USER_DB[username]["role"]
                st.success("登录成功!")
                st.rerun()
            else:
                st.error("用户名或密码错误")


def logout_button():
    st.session_state.logged_in = False
    st.session_state.username = None
    st.session_state.role = None
    st.rerun()


def main():
    """主函数"""
    st.set_page_config(layout="wide", page_title="项目管理", page_icon=":material/edit:")
    if "logged_in" not in st.session_state or not st.session_state.logged_in:
        st.session_state.logged_in = False
        pg = st.navigation([login_page])
    else:
        pg = st.navigation({
            "管理": [
                st.Page("my_pages/deploy_new.py", title="langflow管理", icon=":material/edit:"),
                st.Page("my_pages/fuse_manage.py", title="langfuse管理", icon=":material/edit:"),
                st.Page("my_pages/hh_data.py", title="hh_data管理", icon=":material/edit:"),
            ],
            "账号": [
                st.Page(logout_button, title="登出", icon=":material/logout:")
            ]

        })
    pg.run()


if __name__ == "__main__":
    main()
