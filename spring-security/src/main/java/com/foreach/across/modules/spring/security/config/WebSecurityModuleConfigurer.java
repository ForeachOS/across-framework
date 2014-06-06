package com.foreach.across.modules.spring.security.config;

import org.springframework.security.config.annotation.web.builders.HttpSecurity;

public interface WebSecurityModuleConfigurer
{
	void configure( HttpSecurity http ) throws Exception;
}