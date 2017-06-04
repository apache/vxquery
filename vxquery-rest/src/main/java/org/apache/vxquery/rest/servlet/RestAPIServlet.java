/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.vxquery.rest.servlet;

import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_XML;

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.htrace.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.vxquery.exceptions.VXQueryRuntimeException;
import org.apache.vxquery.exceptions.VXQueryServletRuntimeException;
import org.apache.vxquery.rest.response.APIResponse;
import org.apache.vxquery.rest.response.AsyncQueryResponse;
import org.apache.vxquery.rest.response.ErrorResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.apache.vxquery.rest.response.SyncQueryResponse;
import org.apache.vxquery.rest.service.Status;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Abstract servlet to handle REST API requests.
 *
 * @author Erandi Ganepola
 */
public abstract class RestAPIServlet extends AbstractServlet {

    protected final Logger LOGGER;

    private JAXBContext jaxbContext;

    public RestAPIServlet(ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        LOGGER = Logger.getLogger(this.getClass().getName());
        try {
            jaxbContext = JAXBContext.newInstance(QueryResultResponse.class, AsyncQueryResponse.class,
                    SyncQueryResponse.class, ErrorResponse.class);
        } catch (JAXBException e) {
            LOGGER.log(Level.SEVERE, "Error occurred when creating JAXB context", e);
            throw new VXQueryRuntimeException("Unable to load JAXBContext", e);
        }
    }

    @Override
    protected final void post(IServletRequest request, IServletResponse response) {
        getOrPost(request, response);
    }

    @Override
    protected final void get(IServletRequest request, IServletResponse response) {
        getOrPost(request, response);
    }

    private void getOrPost(IServletRequest request, IServletResponse response) {
        try {
            initResponse(request, response);
            APIResponse entity = doHandle(request);
            if (entity == null) {
                LOGGER.log(Level.WARNING, "No entity found for request : " + request);
                response.setStatus(HttpResponseStatus.BAD_REQUEST);
            } else {
                // Important to set Status OK before setting the entity because the response
                // (chunked) checks it before
                // writing the response to channel.
                setResponseStatus(response, entity);
                setEntity(request, response, entity);
            }
        } catch (IOException e) {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            LOGGER.log(Level.SEVERE, "Error occurred when setting content type", e);
        }
    }

    private void initResponse(IServletRequest request, IServletResponse response) throws IOException {
        // enable cross-origin resource sharing
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");

        HttpUtil.setContentType(response, "text/plain");
    }

    private void setEntity(IServletRequest request, IServletResponse response, APIResponse entity) throws IOException {
        String accept = request.getHeader(HttpHeaderNames.ACCEPT, "");
        String entityString;
        switch (accept) {
            case CONTENT_TYPE_XML:
                try {
                    HttpUtil.setContentType(response, CONTENT_TYPE_XML);

                    Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
                    jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
                    StringWriter sw = new StringWriter();
                    jaxbMarshaller.marshal(entity, sw);
                    entityString = sw.toString();
                } catch (JAXBException e) {
                    LOGGER.log(Level.SEVERE, "Error occurred when mapping java object into xml", e);
                    throw new VXQueryServletRuntimeException("Error occurred when marshalling entity", e);
                }
                break;
            case CONTENT_TYPE_JSON:
            default:
                try {
                    HttpUtil.setContentType(response, CONTENT_TYPE_JSON);
                    ObjectMapper jsonMapper = new ObjectMapper();
                    entityString = jsonMapper.writeValueAsString(entity);
                } catch (JsonProcessingException e) {
                    LOGGER.log(Level.SEVERE, "Error occurred when mapping java object into JSON", e);
                    throw new VXQueryServletRuntimeException("Error occurred when mapping entity", e);
                }
                break;
        }

        response.writer().print(entityString);
    }

    private void setResponseStatus(IServletResponse response, APIResponse entity) {
        if (Status.SUCCESS.toString().equals(entity.getStatus())) {
            response.setStatus(HttpResponseStatus.OK);
        } else if (Status.FATAL.toString().equals(entity.getStatus())) {
            HttpResponseStatus status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            if (entity instanceof ErrorResponse) {
                status = HttpResponseStatus.valueOf(((ErrorResponse) entity).getError().getCode());
            }
            response.setStatus(status);
        }
    }

    /**
     * This abstract method is supposed to return an object which will be the entity
     * of the response being sent to the client. Implementing classes doesn't have
     * to worry about the content type of the request.
     *
     * @param request
     *            {@link IServletRequest} received
     * @return Object to be set as the entity of the response
     */
    protected abstract APIResponse doHandle(IServletRequest request);
}
