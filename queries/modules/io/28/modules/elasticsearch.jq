jsoniq version "1.0";
(:
 : Copyright 2012 28msec Inc.
 :)

(:~
 : <p>This module provides a driver to access
 : <a href="http://www.elasticsearch.org/">ElasticSearch</a>.</p>
 :
 : @author Dennis Knochenwefel
 :)
module namespace es = "http://28.io/modules/elasticsearch";

import module namespace credentials =
    "http://www.28msec.com/modules/credentials";
import module namespace http = "http://zorba.io/modules/http-client";

declare namespace ver = "http://zorba.io/options/versioning";

declare option ver:module-version "1.0";

declare %private variable $es:CREDENTIALS-CATEGORY as xs:string :=
    "ElasticSearch";
declare %private variable $es:RETRY-OPTIONS :=
  {
    retry :
    {
      "delay" : [1000, 2000, 4000],
      "on-connection-error" : true(),
      "on-statuses" : [ 500, 503 ]
    }
  };

declare function es:connection($credentials-name as xs:string?)
as object?
{
  let $credentials as object? := credentials:credentials(
      $es:CREDENTIALS-CATEGORY,
      $credentials-name)
  where exists($credentials)
  return
      let $urls as string+ :=
        for $url in $credentials.urls[]
        return $url || ("/"[not ends-with($url, "/")])
      let $protocol as string := ($credentials.protocol, "https")[1]
      let $user as string? := $credentials.user
      let $password as string? := $credentials.pass
      let $authentication as string? :=
        (
          encode-for-uri($user) || ":" || encode-for-uri($password) || "@"
        )[exists($user) and exists($password)]
      return
        {
          "urls": [
            for $url in $urls
            return $protocol || "://" || $authentication || $url
          ]
        }
};

declare %private function es:url(
  $connection as object,
  $paths as string*
) as string
{
  es:url($connection, $paths, ())
};

declare %private function es:url(
  $connection as object,
  $paths as string*,
  $parameters as string*
) as string
{
  let $server as string := $connection.urls[][1]
  let $parameters as string* :=
    for $parameter in $parameters
    return encode-for-uri($parameter)
  let $path as string := string-join($paths, "/")
  let $query as string := string-join($parameters, "&")
  return
    $server || string-join(($path, $query), "?")
};

declare %private function es:request(
  $method as string,
  $url as string,
  $body as item?
) as object
{
  {|
    {
      "method": $method,
      "href": $url,
      "options": $es:RETRY-OPTIONS
    },
    switch(true)

    case empty($body) return ()

    case $body instance of string
    return
      {
        "body":
        {
          "media-type": "text/plain",
          "content": $body
        }
      }

    case $body instance of object
    case $body instance of array
    return
      {
        "body":
        {
          "media-type": "application/json",
          "content": serialize($body)
        }
      }

    default return
      error(xs:QName("es:UNKNOWN_BODY_TYPE"),
            "cannot create request from body content",
            $body)
  |}
};

declare function es:exists(
  $connection as object,
  $index as string
) as boolean
{
  es:exists($connection, $index, (), ())
};

declare function es:exists(
  $connection as object,
  $index as string,
  $type as string?
) as boolean
{
  es:exists($connection, $index, $type, ())
};

declare function es:exists(
  $connection as object,
  $index as string,
  $type as string?,
  $id as string?
) as boolean
{
  let $url as string := es:url($connection, ($index, $type, $id))
  let $request as object := es:request("HEAD", $url, ())
  let $response as object := http:send-deterministic-request($request)
  return
    switch ($response.status)
    case 200
    case 204 return true
    case 404 return false
    default return error(xs:QName("es:SERVER_ERROR"),
                         $response.body.content,
                         $response)
};

declare %an:sequential function es:create(
  $connection as object,
  $index as string
) as object
{
  es:create($connection, $index, ())
};

declare %an:sequential function es:create(
  $connection as object,
  $index as string,
  $mapping as object?
) as object
{
  let $method as string :=
    if(empty($mapping))
    then "POST"
    else "PUT"
  let $url as string := es:url($connection, ($index))
  let $request as object := es:request($method, $url, $mapping)
  let $response as object := http:send-request($request)
  return
    if($response.status eq 200)
    then parse-json($response.body.content)
    else error(xs:QName("es:SERVER_ERROR"), $response.body.content, $response)
};

declare %an:sequential function es:create(
  $connection as object,
  $index as string,
  $type as string,
  $mapping as object
) as object?
{
  let $url as string := es:url($connection, ($index, "_mapping", $type))
  let $request as object := es:request("PUT", $url, $mapping)
  let $response as object := http:send-request($request)
  return
    if($response.status eq 200)
    then parse-json($response.body.content)
    else error(xs:QName("es:SERVER_ERROR"), $response.body.content, $response)
};

declare %an:sequential function es:delete(
  $connection as object,
  $index as string
) as object?
{
  es:delete($connection, $index, (),())
};

declare %an:sequential function es:delete(
  $connection as object,
  $index as string,
  $type as string?
) as object?
{
  es:delete($connection, $index, $type, ())
};

declare %an:sequential function es:delete(
  $connection as object,
  $index as string,
  $type as string?,
  $id as string?
) as object?
{
  let $url as string := es:url($connection, ($index, $type, $id))
  let $request as object := es:request("DELETE", $url, ())
  let $response as object := http:send-request($request)
  return
    if($response.status eq 200)
    then parse-json($response.body.content)
    else error(xs:QName("es:SERVER_ERROR"), $response.body.content, $response)
};

declare function es:get(
  $connection as object,
  $index as string,
  $type as string,
  $id as string
) as object?
{
  let $url as string := es:url($connection, ($index, $type, $id))
  let $response as object := http:get($url)
  return
    switch ($response.status)
    case 200 return parse-json($response.body.content)
    case 404 return ()
    default return error(xs:QName("es:CONNECTION_ERROR"),
                         $response.body.content,
                         $response)
};

declare %an:sequential function es:insert(
  $connection as object,
  $index as string,
  $type as string,
  $doc as object
) as object?
{
  let $id as string? := $doc._id
  return es:insert($connection, $index, $type, $id, $doc)
};

declare %an:sequential function es:insert(
  $connection as object,
  $index as string,
  $type as string,
  $id as string?,
  $doc as object
) as object?
{
  let $method as string := if(empty($id)) then "POST" else "PUT"
  let $url as string := es:url($connection, ($index, $type, $id))
  let $request as object := es:request($method, $url, $doc)
  let $response as object := http:send-request($request)
  return
    switch ($response.status)
    case 200
    case 201 return parse-json($response.body.content)
    default return error(xs:QName("es:CONNECTION_ERROR"),
                         $response.body.content,
                         $response)
};

declare function es:list(
  $connection as object,
  $index as string,
  $type as string
) as object?
{
  let $url as string := es:url($connection, ($index, $type, "_search"))
  let $request as object := es:request("GET", $url, ())
  let $response as object := http:send-deterministic-request($request)
  return
    if($response.status eq 200)
    then parse-json($response.body.content)
    else error(xs:QName("es:CONNECTION_ERROR"),
                        $response.body.content,
                        $response)
};

declare function es:search(
  $connection as object,
  $index as string,
  $type as string,
  $query as object
) as object?
{
  let $url as string := es:url($connection, ($index, $type, "_search"))
  let $request as object := es:request("GET", $url, $query)
  let $response as object := http:send-deterministic-request($request)
  return
    if($response.status eq 200)
    then parse-json($response.body.content)
    else error(xs:QName("es:CONNECTION_ERROR"),
                        $response.body.content,
                        $response)
};
