--
-- PostgreSQL database dump
--

\restrict DO8iRmxhDj3tcJgQWby2J9x4hhCrOn6mwFPlo8cwvbqm2tSQyhO7qpOgM3lrhhi

-- Dumped from database version 15.15 (Debian 15.15-1.pgdg13+1)
-- Dumped by pg_dump version 15.15 (Debian 15.15-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: bank_rsi_daily; Type: TABLE; Schema: public; Owner: quant_user
--

CREATE TABLE public.bank_rsi_daily (
    ts_code text,
    trade_date date,
    close double precision,
    open double precision,
    rsi double precision,
    price_change double precision,
    rsi_change double precision,
    trend_status text,
    name text
);


ALTER TABLE public.bank_rsi_daily OWNER TO quant_user;

--
-- Name: nt_block_trades; Type: TABLE; Schema: public; Owner: quant_user
--

CREATE TABLE public.nt_block_trades (
    id integer NOT NULL,
    ts_code character varying(10) NOT NULL,
    trade_date date NOT NULL,
    price numeric(10,2),
    vol numeric(18,2),
    amount numeric(18,2),
    buyer character varying(100),
    seller character varying(100)
);


ALTER TABLE public.nt_block_trades OWNER TO quant_user;

--
-- Name: nt_block_trades_id_seq; Type: SEQUENCE; Schema: public; Owner: quant_user
--

CREATE SEQUENCE public.nt_block_trades_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.nt_block_trades_id_seq OWNER TO quant_user;

--
-- Name: nt_block_trades_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: quant_user
--

ALTER SEQUENCE public.nt_block_trades_id_seq OWNED BY public.nt_block_trades.id;


--
-- Name: nt_history_cost; Type: TABLE; Schema: public; Owner: quant_user
--

CREATE TABLE public.nt_history_cost (
    ts_code character varying(20) NOT NULL,
    holder_name character varying(255) NOT NULL,
    hist_cost numeric,
    total_invest numeric,
    total_shares numeric,
    first_buy_date date,
    calc_date timestamp without time zone
);


ALTER TABLE public.nt_history_cost OWNER TO quant_user;

--
-- Name: nt_market_data; Type: TABLE; Schema: public; Owner: quant_user
--

CREATE TABLE public.nt_market_data (
    ts_code character varying(10) NOT NULL,
    trade_date date NOT NULL,
    open numeric(10,2),
    high numeric(10,2),
    low numeric(10,2),
    close numeric(10,2),
    vol numeric(18,2),
    amount numeric(18,2)
);


ALTER TABLE public.nt_market_data OWNER TO quant_user;

--
-- Name: nt_positions_analysis; Type: TABLE; Schema: public; Owner: quant_user
--

CREATE TABLE public.nt_positions_analysis (
    ts_code text,
    name text,
    holder_name text,
    period_end date,
    hold_amount double precision,
    est_cost double precision,
    curr_price double precision,
    profit_rate double precision,
    status text,
    cost_source text,
    first_buy_date timestamp without time zone,
    change_analysis text,
    is_latest boolean,
    update_time timestamp without time zone
);


ALTER TABLE public.nt_positions_analysis OWNER TO quant_user;

--
-- Name: nt_shareholders; Type: TABLE; Schema: public; Owner: quant_user
--

CREATE TABLE public.nt_shareholders (
    id integer NOT NULL,
    ts_code character varying(10) NOT NULL,
    ann_date date,
    end_date date,
    holder_name character varying(100),
    hold_amount numeric(18,2),
    hold_ratio numeric(10,4),
    chg_amount numeric(18,2)
);


ALTER TABLE public.nt_shareholders OWNER TO quant_user;

--
-- Name: nt_shareholders_id_seq; Type: SEQUENCE; Schema: public; Owner: quant_user
--

CREATE SEQUENCE public.nt_shareholders_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.nt_shareholders_id_seq OWNER TO quant_user;

--
-- Name: nt_shareholders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: quant_user
--

ALTER SEQUENCE public.nt_shareholders_id_seq OWNED BY public.nt_shareholders.id;


--
-- Name: nt_stock_fundamentals; Type: TABLE; Schema: public; Owner: quant_user
--

CREATE TABLE public.nt_stock_fundamentals (
    ts_code text,
    total_mv double precision,
    pe_dyn double precision,
    pe_ttm double precision,
    pe_static double precision,
    pb double precision,
    curr_price double precision,
    eps double precision,
    roe double precision,
    div_rate double precision,
    div_rate_static text,
    revenue double precision,
    revenue_growth double precision,
    net_profit_growth double precision,
    gross_margin double precision,
    net_margin double precision,
    update_date date
);


ALTER TABLE public.nt_stock_fundamentals OWNER TO quant_user;

--
-- Name: stock_basic; Type: TABLE; Schema: public; Owner: quant_user
--

CREATE TABLE public.stock_basic (
    ts_code text,
    name text
);


ALTER TABLE public.stock_basic OWNER TO quant_user;

--
-- Name: nt_block_trades id; Type: DEFAULT; Schema: public; Owner: quant_user
--

ALTER TABLE ONLY public.nt_block_trades ALTER COLUMN id SET DEFAULT nextval('public.nt_block_trades_id_seq'::regclass);


--
-- Name: nt_shareholders id; Type: DEFAULT; Schema: public; Owner: quant_user
--

ALTER TABLE ONLY public.nt_shareholders ALTER COLUMN id SET DEFAULT nextval('public.nt_shareholders_id_seq'::regclass);


--
-- Name: nt_block_trades nt_block_trades_pkey; Type: CONSTRAINT; Schema: public; Owner: quant_user
--

ALTER TABLE ONLY public.nt_block_trades
    ADD CONSTRAINT nt_block_trades_pkey PRIMARY KEY (id);


--
-- Name: nt_history_cost nt_history_cost_pkey; Type: CONSTRAINT; Schema: public; Owner: quant_user
--

ALTER TABLE ONLY public.nt_history_cost
    ADD CONSTRAINT nt_history_cost_pkey PRIMARY KEY (ts_code, holder_name);


--
-- Name: nt_market_data nt_market_data_pkey; Type: CONSTRAINT; Schema: public; Owner: quant_user
--

ALTER TABLE ONLY public.nt_market_data
    ADD CONSTRAINT nt_market_data_pkey PRIMARY KEY (ts_code, trade_date);


--
-- Name: nt_shareholders nt_shareholders_pkey; Type: CONSTRAINT; Schema: public; Owner: quant_user
--

ALTER TABLE ONLY public.nt_shareholders
    ADD CONSTRAINT nt_shareholders_pkey PRIMARY KEY (id);


--
-- Name: nt_shareholders nt_shareholders_ts_code_end_date_holder_name_key; Type: CONSTRAINT; Schema: public; Owner: quant_user
--

ALTER TABLE ONLY public.nt_shareholders
    ADD CONSTRAINT nt_shareholders_ts_code_end_date_holder_name_key UNIQUE (ts_code, end_date, holder_name);


--
-- Name: nt_stock_fundamentals uniq_fundamental; Type: CONSTRAINT; Schema: public; Owner: quant_user
--

ALTER TABLE ONLY public.nt_stock_fundamentals
    ADD CONSTRAINT uniq_fundamental UNIQUE (ts_code);


--
-- Name: nt_market_data uniq_market_data; Type: CONSTRAINT; Schema: public; Owner: quant_user
--

ALTER TABLE ONLY public.nt_market_data
    ADD CONSTRAINT uniq_market_data UNIQUE (ts_code, trade_date);


--
-- Name: nt_shareholders uniq_shareholder; Type: CONSTRAINT; Schema: public; Owner: quant_user
--

ALTER TABLE ONLY public.nt_shareholders
    ADD CONSTRAINT uniq_shareholder UNIQUE (ts_code, holder_name, end_date);


--
-- Name: stock_basic uniq_stock_basic; Type: CONSTRAINT; Schema: public; Owner: quant_user
--

ALTER TABLE ONLY public.stock_basic
    ADD CONSTRAINT uniq_stock_basic UNIQUE (ts_code);


--
-- PostgreSQL database dump complete
--

\unrestrict DO8iRmxhDj3tcJgQWby2J9x4hhCrOn6mwFPlo8cwvbqm2tSQyhO7qpOgM3lrhhi

