import re
import streamlit as st
import time
import logging
logger = logging.getLogger(__name__)

from app import (
    load_semantic_documentation,
    authenticate_powerbi,
    identify_question_intent,
    explain_dashboard,
    understand_question,
    understand_navigation,
    identify_relevant_dashboard_page,
    DASHBOARD_PAGES,
    validate_request,
    resolve_request,
    build_dax_query,
    query_powerbi,
    explain_result,
    execute_scenario_projection,
)


# --------------------------------------------------
# 1. Page configuration
# --------------------------------------------------

st.set_page_config(
    page_title="Airport Analytics AI Assistant",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.title("✈️ Airport Analytics AI Assistant")

st.caption(
    "Explore airport performance through interactive dashboards "
    "and AI-powered analytics."
)


# --------------------------------------------------
# 2. Load semantic documentation
# --------------------------------------------------

@st.cache_resource
def load_documentation():
    return load_semantic_documentation()


documentation = load_documentation()


# --------------------------------------------------
# 3. Power BI authentication
# --------------------------------------------------

if "powerbi_token" not in st.session_state:
    with st.spinner("Connecting to Power BI..."):
        st.session_state.powerbi_token = authenticate_powerbi()

token = st.session_state.powerbi_token


# --------------------------------------------------
# 4. Initialize chat history and conversation state
# --------------------------------------------------

if "messages" not in st.session_state:
    st.session_state.messages = []

if "pending_scenario" not in st.session_state:
    st.session_state.pending_scenario = None

if "selected_page" not in st.session_state:
    st.session_state.selected_page = "Overview"


# --------------------------------------------------
# 5. Dashboard / chatbot layout
# --------------------------------------------------

show_chatbot = st.toggle(
    "🤖 Show AI Assistant",
    value=True,
)

if show_chatbot:
    dashboard_col, chatbot_col = st.columns(
        [3, 1],
        gap="medium",
    )
else:
    dashboard_col = st.container()
    chatbot_col = None


# --------------------------------------------------
# 6. Power BI Dashboard Configuration
# --------------------------------------------------

POWERBI_EMBED_URL = (
    "https://app.fabric.microsoft.com/reportEmbed"
    "?reportId=b4126f50-f895-4d58-9939-e2c3f31cce9a"
    "&autoAuth=true"
    "&ctid=921edb2e-7951-47a6-959e-56e1b43593e8"
    f"&pageName={DASHBOARD_PAGES[st.session_state.selected_page]}"
)


# --------------------------------------------------
# 6.1 Power BI dashboard (left)
# --------------------------------------------------

with dashboard_col:

    st.subheader("📊 Airport Operations Dashboard")

    st.iframe(
        POWERBI_EMBED_URL,
        height=750,
    )


# --------------------------------------------------
# 7. AI chatbot (right)
# --------------------------------------------------

logger = logging.getLogger(__name__)


# --------------------------------------------------
# 7.1 User-friendly error handling
# --------------------------------------------------

def format_user_error(error):
    """
    Convert backend exceptions into clear messages
    for airport analytics users.

    The original traceback remains available
    in the VS Code terminal through logger.exception().
    """

    message = str(error).lower()

    # ------------------------------------------
    # 1. Missing reporting period
    # ------------------------------------------

    if any(
        phrase in message
        for phrase in (
            "specify a month",
            "reporting period",
            "month and year",
        )
    ):
        return (
            "📅 Please specify a reporting period, "
            "such as **August 2026**."
        )

    # ------------------------------------------
    # 2. Missing KPI baseline
    # ------------------------------------------

    if any(
        phrase in message
        for phrase in (
            "baseline is missing",
            "missing baseline",
            "no baseline",
            "baseline is blank",
            "not numeric",
            "no data",
            "empty result",
        )
    ):
        return (
            "📊 No usable actual KPI value was found "
            "for the requested reporting period.\n\n"
            "Please select another month or KPI. "
            "A scenario projection requires an actual "
            "baseline from Power BI."
        )

    # ------------------------------------------
    # 3. Unsupported KPI
    # ------------------------------------------

    if any(
        phrase in message
        for phrase in (
            "unsupported metric",
            "unknown metric",
            "unsupported kpi",
            "no valid semantic-model measure",
        )
    ):
        return (
            "🔎 I couldn't identify a supported KPI "
            "in your request.\n\n"
            "Please choose a metric available "
            "in the airport analytics dashboard."
        )

    # ------------------------------------------
    # 4. Scenario interpretation failure
    # ------------------------------------------

    if any(
        phrase in message
        for phrase in (
            "couldn't reliably interpret",
            "could not match every stated change",
            "invalid assumption",
            "invalid scenario",
            "multiple adjustments",
            "duplicate kpi",
            "unsupported operation",
        )
    ):
        return (
            "⚠️ I couldn't reliably interpret your "
            "scenario assumptions.\n\n"
            "Please specify a supported KPI, "
            "the proposed adjustment, and a "
            "reporting period.\n\n"
            "**Example:** What if commercial revenue "
            "increases by 10% in August 2026?"
        )

    # ------------------------------------------
    # 5. Power BI connection errors
    # ------------------------------------------

    if any(
        phrase in message
        for phrase in (
            "unauthorized",
            "forbidden",
            "connection error",
            "connectionerror",
            "token expired",
            "401 client error",
            "403 client error",
            "429 client error",
            "500 server error",
            "502 server error",
            "503 server error",
            "504 server error",
        )
    ):
        return (
            "🔌 I'm temporarily unable to retrieve "
            "data from Power BI.\n\n"
            "Please try again shortly."
        )

    # ------------------------------------------
    # 6. Unexpected errors
    # ------------------------------------------

    return (
        "⚠️ Something went wrong while processing "
        "your request.\n\n"
        "Please try again or rephrase your question."
    )


# --------------------------------------------------
# 7.2 AI chatbot interface
# --------------------------------------------------

if show_chatbot:

    with chatbot_col:

        st.subheader("👨‍✈️ AI Analytics Assistant")

        # Welcome message
        if not st.session_state.messages:
            with st.chat_message("assistant", avatar="👨‍✈️"):
                st.markdown(
                    """
                    **Welcome aboard! I'm your Airport Operations Analytics Assistant.**

                    I can help you:

                    - 📊 Understand dashboard insights and KPIs.
                    - 🧭 Navigate airport performance reports.
                    - 📈 Analyze actual operational performance.
                    - 🔮 Explore hypothetical scenarios and projections.

                    **How can I assist you today?**
                    """
                )

        # Display conversation history
        for message in st.session_state.messages:

            avatar = (
                "👨‍✈️"
                if message["role"] == "assistant"
                else None
            )

            with st.chat_message(
                message["role"],
                avatar=avatar,
            ):
                st.markdown(message["content"])

        # User input
        question = st.chat_input(
            "Ask about airport operations..."
        )

        if question:

            user_question = question
            should_navigate = False

            # ------------------------------------------
            # Conversational scenario follow-ups
            # ------------------------------------------

            date_pattern = (
                r"^\s*(January|February|March|April|May|June|"
                r"July|August|September|October|November|December)"
                r"\s+\d{4}\s*$"
            )

            is_date_followup = bool(
                re.fullmatch(
                    date_pattern,
                    question,
                    re.IGNORECASE,
                )
            )

            if (
                st.session_state.pending_scenario
                and is_date_followup
            ):

                question = (
                    f"{st.session_state.pending_scenario} "
                    f"in {question.strip()}"
                )

                st.session_state.pending_scenario = None

            elif st.session_state.pending_scenario:

                st.session_state.pending_scenario = None

            # Save and display original user question
            st.session_state.messages.append(
                {
                    "role": "user",
                    "content": user_question,
                }
            )

            with st.chat_message("user"):
                st.markdown(user_question)

            # ------------------------------------------
            # Process assistant response
            # ------------------------------------------

            with st.chat_message(
                "assistant",
                avatar="👨‍✈️",
            ):

                with st.spinner("Analyzing your request..."):

                    start_time = time.perf_counter()

                    try:

                        intent = identify_question_intent(
                            question
                        )

                        # ----------------------------------
                        # Capability 2: Dashboard navigation
                        # ----------------------------------

                        if intent == "navigation":

                            navigation = understand_navigation(
                                question,
                                documentation,
                            )

                            page = navigation.get("page")
                            action = navigation.get("action")

                            if page is None:

                                answer = (
                                    "I couldn't identify a matching "
                                    "dashboard page. Please specify "
                                    "the operational area you want "
                                    "to explore."
                                )

                            elif page not in DASHBOARD_PAGES:

                                answer = (
                                    "The requested dashboard page "
                                    "is not available."
                                )

                            elif action == "open":

                                if st.session_state.selected_page != page:

                                    st.session_state.selected_page = page
                                    should_navigate = True

                                answer = (
                                    f"Opening the **{page}** "
                                    "dashboard page."
                                )

                            else:

                                answer = (
                                    "You can explore this information "
                                    f"on the **{page}** dashboard page."
                                )

                        # ----------------------------------
                        # Capability 1: Dashboard explanation
                        # ----------------------------------

                        elif intent == "explanation":

                            answer = explain_dashboard(
                                question,
                                documentation,
                            )

                        # ----------------------------------
                        # Capability 3: Scenario projection
                        # ----------------------------------

                        elif intent == "simulation":

                            has_period = bool(
                                re.search(
                                    r"\b(January|February|March|April|"
                                    r"May|June|July|August|September|"
                                    r"October|November|December)"
                                    r"\s+\d{4}\b",
                                    question,
                                    re.IGNORECASE,
                                )
                            )

                            if not has_period:

                                st.session_state.pending_scenario = question

                                answer = (
                                    "📅 Which reporting period would "
                                    "you like to use? Please specify "
                                    "a month and year, such as "
                                    "**August 2026**."
                                )

                            else:

                                scenario_output = execute_scenario_projection(
                                    question,
                                    token,
                                )

                                answer = scenario_output["answer"]

                                relevant_page = scenario_output.get("page")

                                if (
                                    relevant_page
                                    and relevant_page in DASHBOARD_PAGES
                                ):

                                    if (
                                        st.session_state.selected_page
                                        != relevant_page
                                    ):

                                        st.session_state.selected_page = (
                                            relevant_page
                                        )

                                        should_navigate = True

                                        answer += (
                                            "\n\n📊 I've opened the "
                                            f"**{relevant_page}** dashboard "
                                            "page for further exploration."
                                        )

                        # ----------------------------------
                        # Capability 1: Actual KPI analysis
                        # ----------------------------------

                        else:

                            raw_request = understand_question(
                                question,
                                documentation,
                            )

                            request = validate_request(
                                question,
                                raw_request,
                            )

                            resolved_request = resolve_request(
                                question,
                                request,
                            )

                            if not resolved_request.get("supported"):

                                answer = resolved_request.get(
                                    "message",
                                    "This question is outside "
                                    "the airport analytics scope.",
                                )

                            else:

                                dax_query = build_dax_query(
                                    resolved_request
                                )

                                result = query_powerbi(
                                    token,
                                    dax_query,
                                )

                                answer = explain_result(
                                    question,
                                    resolved_request,
                                    result,
                                    documentation,
                                )

                                relevant_page = (
                                    identify_relevant_dashboard_page(
                                        question
                                    )
                                )

                                if (
                                    relevant_page
                                    and relevant_page in DASHBOARD_PAGES
                                ):

                                    if (
                                        st.session_state.selected_page
                                        != relevant_page
                                    ):

                                        st.session_state.selected_page = (
                                            relevant_page
                                        )

                                        should_navigate = True

                                        answer += (
                                            "\n\n📊 I've opened the "
                                            f"**{relevant_page}** dashboard "
                                            "page for further exploration."
                                        )

                    except Exception as error:

                        logger.exception(
                            "Airport Analytics Assistant request failed"
                        )

                        answer = format_user_error(error)

                # Display assistant response
                elapsed_time = time.perf_counter() - start_time

                st.markdown(answer)

                st.caption(
                    f"⏱️ Processing time: {elapsed_time:.2f} seconds"
                )



            # Save assistant response
            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": answer,
                }
            )

            # Refresh dashboard when navigation occurs
            if should_navigate:
                st.rerun()