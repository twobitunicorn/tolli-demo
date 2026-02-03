import warnings
import random
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


st.markdown(
    """
<style>
    /* Target the specific div that wraps the image and remove border-radius */
    .st-emotion-cache-1dp5vir { /* This class name is specific to Streamlit's internal structure and might change with updates */
        border-radius: 0px !important;
    }
    /* A more generic approach that may work across versions: */
    /* Target the image element within the main block container */
    .block-container img {
        border-radius: 0px !important;
    }
</style>
""",
    unsafe_allow_html=True,
)


st.set_page_config(page_title="Tolli.ai", page_icon="favicon.svg", layout="wide")

col_buffer_left, col_content, col_buffer_right = st.columns([1, 10, 1])


team_frame = get_teams_data()
contributor_frame = get_contributors_data()

with col_content:
    st.image("logoLight.svg")
    st.title("Demo (v.0.0.11)")
    st.text(
        "This is an industry overview as processed from various organizations.  All information is public.  This model is still in development and result should not be taken as fact."
    )
    with st.container():
        st.header("Clustering")
        st.text(
            "This is where we show the attributes of each team and engineer.  The tabs give some debug insight."
        )

        _teams = {_team["slug"]: _team for _team in team_frame.rows(named=True)}
        teams = st.multiselect(
            label="Choose teams",
            options=list(_teams.keys()),
            default=["microsoft", "facebook", "flutter"],
            format_func=lambda x: _teams[x]["name"],
        )
        if not teams:
            st.error("Please select at least one team.")
        else:
            for team in teams:
                with st.container():
                    with st.container(horizontal=True):
                        st.image(_teams[team]["avatar_url"], width=68)
                        st.space("xxsmall")
                        st.subheader(_teams[team]["name"])
                    trend_tab, variance_tab, cluster_tab = st.tabs(
                        ["Trend", "Variance", "Clustering"]
                    )
                    with st.expander("Contributors"):
                        for contributor in contributor_frame.filter(
                            pl.col("current_team_slug") == team
                        ).rows(named=True):
                            with st.container(horizontal=True):
                                st.space("xsmall")
                                st.image(contributor["avatar_url"], width=42)
                                st.space("xxsmall")
                                st.text(
                                    contributor["display_name"] or contributor["login"]
                                )

            # for contributor in (
            #     get_contributors_data()
            #     .filter(pl.col("current_team_slug") == team)
            #     .rows(named=True)
            # ):
            #     buf, col1, col2 = st.columns([1, 1, 10])
            #     with col1:
            #         st.image(contributor["avatar_url"], width=68)
            #     with col2:
            #         st.subheader(contributor["display_name"])

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
    with st.container():
        st.header("Team Construction")
        st.text(
            "This is where we show the attributes of each team and engineer.  The tabs give some debug insight."
        )

        _contributors = {
            _contributor["login"]: _contributor
            for _contributor in contributor_frame.rows(named=True)
        }
        contributors = st.multiselect(
            label="Choose contributors",
            options=list(_contributors.keys()),
            default=random.sample(list(_contributors.keys()), 7),
            format_func=lambda x: _contributors[x]["display_name"]
            or _contributors[x]["login"],
        )
        if not contributors:
            st.error("Please select at least one contributors.")
        else:
            pass
