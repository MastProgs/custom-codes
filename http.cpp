// cppTest.cpp : 이 파일에는 'main' 함수가 포함됩니다. 거기서 프로그램 실행이 시작되고 종료됩니다.
// https://github.com/yhirose/cpp-httplib#get-with-http-headers

#include <iostream>
#include <sstream>
#include <vector>
#include <format>
#include <atlstr.h>
#include <comdef.h>
#include <functional>
#include <future>
#include <memory>
#include <vector>

#include "json.hpp"


#include "httplib.h"

class WebServer
{
public:
	WebServer() {};
	~WebServer() {};

	std::vector<std::string> tokenize(std::string const& str, const char delim)
	{
		std::vector<std::string> out;

		// construct a stream from the string 
		std::stringstream ss(str);
		std::string s;
		while (std::getline(ss, s, delim))
		{
			out.emplace_back(s);
		}

		return out;
	}

	virtual std::string Url()
	{
		return "";
	};

private:
	std::string m_ip = "127.0.0.1";// "3.37.173.184";
	int m_port = 10002;

public:
	std::shared_ptr<httplib::Client> http = std::make_shared<httplib::Client>(m_ip, m_port);
	std::string contentType = "application/json";
};

#define WEB_URL_DEFINE(prev, url) \
class url : public prev \
{ \
public: \
	url() \
	{ \
		myName = tokenize(typeid(this).name(), ' ')[1]; \
		if (std::string::npos != myName.find(':')) \
		{ \
			myName = tokenize(myName, ':')[2]; \
		} \
	}; \
	~url() {}; \
 \
	virtual std::string Url()  \
	{ \
		return __super::Url() + "/" + myName; \
	}; \
\
	template <typename... ARGS> \
	auto Get(std::function<void(const httplib::Result&)> callback, ARGS... args) \
	{ \
		auto f = [s = http, url = Url(), callback = callback, ...arg = std::forward<ARGS>(args)](){ \
			callback(s->Get(url.c_str(), arg...)); \
		}; \
		std::thread{ f }.detach(); \
	} \
\
	template <typename... ARGS> \
	auto FGet(std::function<void(const httplib::Result&)> callback, ARGS... args) \
	{ \
		auto f = [s = http, url = Url(), callback = callback, ...arg = std::forward<ARGS>(args)](){ \
			callback(s->Get(url.c_str(), arg...)); \
		}; \
		return f; \
	} \
\
	template <typename... ARGS> \
	auto Post(std::function<void(const httplib::Result&)> callback, ARGS... args) \
	{ \
		auto f = [s = http, url = Url(), callback = callback, ...arg = std::forward<ARGS>(args)](){ \
			callback(s->Post(url.c_str(), arg...)); \
		}; \
		std::thread{ f }.detach(); \
	} \
\
	auto Post(std::function<void(const httplib::Result&)> callback, const std::string& body) \
	{ \
		auto f = [s = http, url = Url(), callback = callback, body = body, contentType = contentType](){ \
			callback(s->Post(url.c_str(), body, contentType.c_str())); \
		}; \
		std::thread{ f }.detach(); \
	} \
\
	template <typename... ARGS> \
	auto FPost(std::function<void(const httplib::Result&)> callback, ARGS... args) \
	{ \
		auto f = [s = http, url = Url(), callback = callback, ...arg = std::forward<ARGS>(args)](){ \
			callback(s->Post(url.c_str(), arg...)); \
		}; \
		return f; \
	} \
\
	auto FPost(std::function<void(const httplib::Result&)> callback, const std::string& body) \
	{ \
		auto f = [s = http, url = Url(), callback = callback, body = body, contentType = contentType](){ \
			callback(s->Post(url.c_str(), body, contentType.c_str())); \
		}; \
		return f; \
	} \
\
private: \
	std::string myName; \
};


namespace HTTP
{
	WEB_URL_DEFINE(WebServer, v1)
		WEB_URL_DEFINE(v1, ping)
			WEB_URL_DEFINE(ping, post)
			WEB_URL_DEFINE(ping, test)
			WEB_URL_DEFINE(ping, json)

	/*class json : public ping
	{
	public:
		json()
		{
			myName = tokenize(typeid(this).name(), ' ')[1];
			if (std::string::npos != myName.find(':'))
			{
				myName = tokenize(myName, ':')[2];
			}
		};
		~json() {};

		virtual std::string Url()
		{
			return __super::Url() + "/" + myName;
		};

		template <typename... ARGS>
		auto Get(std::function<void(const httplib::Result&)> callback, ARGS... args)
		{
			auto f = [s = http, url = Url(), callback = callback, ...arg = std::forward<ARGS>(args)](){
				callback(s->Get(url.c_str(), arg...));
			};
			std::thread{ f }.detach();
		}

		template <typename... ARGS>
		auto FGet(std::function<void(const httplib::Result&)> callback, ARGS... args)
		{
			auto f = [s = http, url = Url(), callback = callback, ...arg = std::forward<ARGS>(args)](){
				callback(s->Get(url.c_str(), arg...));
			};
			return f;
		}

		template <typename... ARGS>
		auto Post(std::function<void(const httplib::Result&)> callback, ARGS... args)
		{
			auto f = [s = http, url = Url(), callback = callback, ...arg = std::forward<ARGS>(args)](){
				callback(s->Post(url.c_str(), arg...));
			};
			std::thread{ f }.detach();
		}

		auto Post(std::function<void(const httplib::Result&)> callback, const std::string& body)
		{
			auto f = [s = http, url = Url(), callback = callback, body = body, contentType = contentType](){
				callback(s->Post(url.c_str(), body, contentType.c_str()));
			};
			std::thread{ f }.detach();
		}

		template <typename... ARGS>
		auto FPost(std::function<void(const httplib::Result&)> callback, ARGS... args)
		{
			auto f = [s = http, url = Url(), callback = callback, ...arg = std::forward<ARGS>(args)](){
				callback(s->Post(url.c_str(), arg...));
			};
			return f;
		}

		auto FPost(std::function<void(const httplib::Result&)> callback, const std::string& body)
		{
			auto f = [s = http, url = Url(), callback = callback, body = body, contentType = contentType](){
				callback(s->Post(url.c_str(), body, contentType.c_str()));
			};
			return f;
		}

	private:
		std::string myName;
	};*/
}

int main()
{
	// open source info
	// http - https://github.com/yhirose/cpp-httplib
	// Json - https://github.com/nlohmann/json#license

	std::vector<std::thread> v_thread;

	for (int i = 0; i < 1000; i++)
	{
		{
			// http 처리
			HTTP::test p;

			auto f = p.FGet([i = i, url = p.Url()](const httplib::Result& res) {
				if (res->status == 200) { std::cout << i << " GET - " << res->body << "\n"; }
				else { std::cout << std::format("[ERROR] http : {} - {}, URL = {}\n", res->status, res->reason, url); }
			});

			v_thread.emplace_back(std::thread{ f });
		}

		{
			HTTP::post p;

			auto f = p.FPost([i = i, url = p.Url()](const httplib::Result& res) {
				if (res->status == 200) { std::cout << i << " POST - " << res->body << "\n"; }
				else { std::cout << std::format("[ERROR] http : {} - {}, URL = {}\n", res->status, res->reason, url); }
			});

			v_thread.emplace_back(std::thread{ f });
		}

		{
			HTTP::json p;

			nlohmann::json j;
			j["num"] = i;

			p.Post([i = i, url = p.Url()](const httplib::Result& res) {
				if (res->status == 200) 
				{
					auto parse = nlohmann::json::parse(res->body);
					auto num = parse["num"];
					int num2 = num;
					std::cout << i << " JSON - RAW = " << res->body << ", Value = " << num << " :: " << num2 << "\n";
				}
				else { std::cout << std::format("[ERROR] http : {} - {}, URL = {}\n", res->status, res->reason, url); }
			}, j.dump());

			//v_thread.emplace_back(std::thread{ f });
		}
	}

	for (auto& th : v_thread)
	{
		th.join();
	}
}
