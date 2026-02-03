import warnings
from urllib.error import URLError

import polars as pl
import s3fs
import streamlit as st
from dotenv import load_dotenv

AWS_ACCESS_KEY_ID = "tid_xpRCdsbJTvJoNOJzygerAAVUDPlKVvGwGOCLwyuPvsZUnBnrEC"
AWS_SECRET_ACCESS_KEY = (
    "tsec_lxcWdBpInYBlBfNw+kz7ySYDm+u+AsJW3eVGypI7Q3BUeyX8RfPPg2vFHO5jzI89XrvXJb"
)
AWS_ENDPOINT_URL_S3 = "https://t3.storage.dev"
# AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
AWS_ENDPOINT_URL_IAM = "https://iam.storage.dev"
AWS_REGION = "auto"
BUCKET_NAME = "damp-shape-1501"

s3 = s3fs.S3FileSystem(
    endpoint_url=AWS_ENDPOINT_URL_S3,
    key=AWS_ACCESS_KEY_ID,
    secret=AWS_SECRET_ACCESS_KEY,
)
warnings.filterwarnings("ignore")

load_dotenv()


@st.cache_data
def get_teams_data() -> pl.DataFrame:
    with s3.open(f"{BUCKET_NAME}/tolli/team_frame.ipc", "rb") as f:
        return pl.read_ipc_stream(f)


@st.cache_data
def get_contributors_data() -> pl.DataFrame:
    with s3.open(f"{BUCKET_NAME}/tolli/contributor_frame.ipc", "rb") as f:
        return pl.read_ipc_stream(f)


st.set_page_config(layout="wide")

try:
    col_buffer_left, col_content, col_buffer_right = st.columns([1, 10, 1])
    with col_content:
        team_frame = get_teams_data()
        title = st.title("Tolli.ai Demo (v.0.0.11)")
        _teams = {_team["slug"]: _team for _team in team_frame.rows(named=True)}
        teams = st.multiselect(
            label="Choose teams",
            options=list(_teams.keys()),
            default=list(_teams.keys()),
            format_func=lambda x: _teams[x]["name"],
        )
        if not teams:
            st.error("Please select at least one team.")
        else:
            for team in teams:
                col1, col2, _ = st.columns([1, 1, 10])
                with col1:
                    st.image(_teams[team]["avatar_url"], width=68)
                with col2:
                    st.subheader(_teams[team]["name"])

                for contributor in (
                    get_contributors_data()
                    .filter(pl.col("current_team_slug") == team)
                    .rows(named=True)
                ):
                    buf, col1, col2 = st.columns([1, 1, 10])
                    with col1:
                        st.image(contributor["avatar_url"], width=68)
                    with col2:
                        st.subheader(contributor["display_name"])

        # data: Unknown = df.loc[countries]
        # data /= 1000000.0
        # st.subheader("Gross agricultural production ($B)")
        # st.dataframe(data.sort_index())

        # data = data.T.reset_index()
        # data = pd.melt(data, id_vars=["index"]).rename(
        #     columns={"index": "year", "value": "Gross Agricultural Product ($B)"}
        # )
        # chart = (
        #     alt.Chart(data)
        #     .mark_area(opacity=0.3)
        #     .encode(
        #         x="year:T",
        #         y=alt.Y("Gross Agricultural Product ($B):Q", stack=None),
        #         color="Region:N",
        #     )
        # )
        # st.altair_chart(chart, use_container_width=True)
except URLError as e:
    st.error(f"This demo requires internet access. Connection error: {e.reason}")
